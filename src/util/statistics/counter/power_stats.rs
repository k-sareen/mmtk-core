use std::collections::HashMap;
use std::f32::consts::E;
use std::fs::File;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PowerStatsChannel {
    pub id: i32,
    pub name: String,
    pub subsystem: String,
}

#[allow(non_snake_case)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PowerStatsEnergyMeasurement {
    pub id: i32,
    pub timestamp_ms: u64,
    pub duration_ms: u64,
    pub energy_uW_s: u64,
}

pub struct PowerStats {
    pub device_paths: HashMap<PathBuf, String>,
    pub channel_ids: HashMap<String, i32>,
    pub channel_infos: Vec<PowerStatsChannel>,
    pub reading: Vec<PowerStatsEnergyMeasurement>,
    prev_reading: Vec<PowerStatsEnergyMeasurement>,
    pub reading_stw: Vec<PowerStatsEnergyMeasurement>,
    prev_reading_stw: Vec<PowerStatsEnergyMeasurement>,
}

/// Find the earliest position in `text` (starting at `start`) of any character in `chars`.
/// Returns None if no such character is found.
fn find_first_of(text: &str, chars: &str, start: usize) -> Option<usize> {
    // Safely slice from `start` to the end
    let sliced = text.get(start..)?;
    // Check each char in that slice
    for (offset, c) in sliced.char_indices() {
        if chars.contains(c) {
            // Return the position in the original string
            return Some(start + offset);
        }
    }
    None
}

fn split(s: &str, delimiters: &str) -> Vec<String> {
    assert!(delimiters.len() != 0);
    let mut result = Vec::new();

    let mut base = 0;
    let size = s.len();
    let mut found: Option<usize>;

    loop {
        found = find_first_of(s, delimiters, base);
        match found {
            Some(pos) => {
                if pos > base {
                    result.push(s.get(base..pos).unwrap().to_string());
                }
                base = pos + 1;
            }
            None => {
                result.push(s.get(base..size).unwrap().to_string());
                break;
            }
        }
    }
    result
}

impl PowerStatsChannel {
    pub fn new(id: i32, name: String, subsystem: String) -> Self {
        PowerStatsChannel {
            id,
            name,
            subsystem,
        }
    }
}

impl PowerStatsEnergyMeasurement {
    #[allow(non_snake_case)]
    pub fn new(id: i32, timestamp_ms: u64, duration_ms: u64, energy_uW_s: u64) -> Self {
        PowerStatsEnergyMeasurement {
            id,
            timestamp_ms,
            duration_ms,
            energy_uW_s,
        }
    }
}

impl PowerStats {
    const DEVICE_TYPE: &str = "iio:device";
    const IIO_ROOT_DIR: &str = "/sys/bus/iio/devices";
    const NAME_DIR: &str = "name";
    const ENABLED_RAILS_NODE: &str = "enabled_rails";
    const ENERGY_VALUE_NODE: &str = "energy_value";

    pub fn new() -> Self {
        PowerStats {
            device_paths: HashMap::new(),
            channel_ids: HashMap::new(),
            channel_infos: Vec::new(),
            reading: Vec::new(),
            prev_reading: Vec::new(),
            reading_stw: Vec::new(),
            prev_reading_stw: Vec::new(),
        }
    }

    pub fn init(&mut self, device_names: &[&str]) {
        self.find_iio_energy_meter_nodes(device_names);
        self.parse_enabled_rails();
        self.reading.resize(
            self.channel_infos.len(),
            PowerStatsEnergyMeasurement::default(),
        );
        self.reading_stw.resize(
            self.channel_infos.len(),
            PowerStatsEnergyMeasurement::default(),
        );
    }

    fn find_iio_energy_meter_nodes(&mut self, device_names: &[&str]) {
        let files = std::fs::read_dir(Self::IIO_ROOT_DIR);
        if files.is_err() {
            error!("Failed to read directory {}", Self::IIO_ROOT_DIR);
            return;
        }

        let files = files.unwrap();
        for entry in files {
            match entry {
                Ok(entry) => {
                    let file_name = entry.file_name();
                    let file_name = file_name.to_str().unwrap();
                    if file_name.find(Self::DEVICE_TYPE).is_some() {
                        let device_path = entry.path();
                        let device_name = std::fs::read_to_string(device_path.join(Self::NAME_DIR));
                        if let Ok(name) = device_name {
                            for allowed_name in device_names {
                                if name.find(allowed_name).is_some() {
                                    self.device_paths.insert(device_path.clone(), name.clone());
                                }
                            }
                        } else {
                            error!("Failed to read device name for {}", device_path.display());
                            error!("Error: {}", device_name.err().unwrap());
                        }
                    }
                }
                Err(_) => {
                    error!("Failed to read entry in {}", Self::IIO_ROOT_DIR);
                }
            }
        }
    }

    fn parse_enabled_rails(&mut self) {
        let mut id: i32 = 0;
        for (device_path, device_name) in &self.device_paths {
            let enabled_rails_path = device_path.join(Self::ENABLED_RAILS_NODE);
            let enabled_rails = std::fs::read_to_string(enabled_rails_path.clone());
            if let Err(e) = enabled_rails {
                error!(
                    "Failed to read enabled rails for device {}: {}",
                    device_name,
                    enabled_rails_path.display()
                );
                error!("Error: {}", e);
                continue;
            }
            let enabled_rails = enabled_rails.unwrap();
            let enabled_rails = enabled_rails.split('\n');
            for line in enabled_rails {
                if line.is_empty() {
                    continue;
                }
                let words = split(line, ":][");
                if words.len() == 3 {
                    let channel_name = words[1].to_string();
                    let subsystem_name = words[2].to_string();
                    if self.channel_ids.contains_key(&channel_name) {
                        error!(
                            "kunals: There exists rails with the same name (not supported): {}. Only the last occurrence of rail energy will be provided.",
                            channel_name,
                        );
                        continue;
                    }
                    self.channel_infos.push(PowerStatsChannel::new(
                        id,
                        channel_name.clone(),
                        subsystem_name,
                    ));
                    self.channel_ids.insert(channel_name, id);
                    id += 1;
                } else {
                    error!(
                        "Unexpected enabled rail format in {}:\n  {}",
                        device_path.display(),
                        line,
                    );
                }
            }
        }
    }

    fn parse_energy_value(&mut self, path: &PathBuf) -> Result<(), String> {
        let energy_data = std::fs::read_to_string(path.join(Self::ENERGY_VALUE_NODE));
        if energy_data.is_err() {
            error!("Failed to read energy value for {}", path.display());
        }

        self.parse_energy_contents(energy_data.unwrap())
    }

    #[allow(non_snake_case)]
    fn parse_energy_contents(&mut self, energy_data: String) -> Result<(), String> {
        let mut timestamp: u64 = 0;
        let mut timestamp_read = false;

        let lines = energy_data.split('\n');
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let mut parse_line_success = false;
            if !timestamp_read {
                match line[2..].parse::<u64>() {
                    Ok(ts) => {
                        timestamp = ts;
                        if timestamp == 0 || timestamp == std::u64::MAX {
                            error!("Invalid timestamp in {}", line);
                            continue;
                        }

                        timestamp_read = true;
                        parse_line_success = true;
                    }
                    Err(_) => {
                        error!("Failed to parse timestamp in {}", line);
                    }
                }
            } else {
                use regex::Regex;

                let re = Regex::new(r"CH(\d+)\(T=(\d+)\)\[(\S+)\], (\d+)").unwrap();
                for cap in re.captures_iter(line) {
                    let duration = &cap[2];
                    let rail_name = &cap[3];
                    let energy = &cap[4];
                    if self.channel_ids.contains_key(rail_name) {
                        let index = self.channel_ids[rail_name] as usize;
                        self.reading[index].id = index as i32;
                        self.reading[index].timestamp_ms = timestamp;
                        self.reading[index].duration_ms = duration.parse::<u64>().unwrap();
                        self.reading[index].energy_uW_s = energy.parse::<u64>().unwrap();

                        if self.reading[index].energy_uW_s == std::u64::MAX {
                            error!("Invalid energy value on rail {}", rail_name);
                        }
                    }
                    parse_line_success = true;
                }
            }

            if !parse_line_success {
                return Err(format!("Failed to parse line: {}", line));
            }
        }
        Ok(())
    }

    fn read_energy_meter(&mut self, ids: &[i32]) -> Vec<PowerStatsEnergyMeasurement> {
        let device_paths = self.device_paths.clone();
        for device_path in device_paths.keys() {
            if self.parse_energy_value(device_path).is_err() {
                error!("Failed to read energy value for {}", device_path.display());
                return vec![];
            }
        }

        if ids.len() == 0 {
            return self.reading.clone();
        } else {
            let mut result = Vec::with_capacity(ids.len());
            for id in ids {
                if *id < 0 || *id as usize >= self.channel_infos.len() {
                    error!("Invalid channel id: {}", id);
                    return vec![];
                } else {
                    result.push(self.reading[*id as usize]);
                }
            }
            return result;
        }
        vec![]
    }

    fn get_energy_meter_info(&self) -> &Vec<PowerStatsChannel> {
        &self.channel_infos
    }

    pub fn start_all(&mut self) {
        let energy_stats = self.read_energy_meter(&[]);
        self.prev_reading = energy_stats;
    }

    pub fn stop_all(&mut self) {
        self.read_energy_meter(&[]);
    }

    pub fn start_gc(&mut self) {
        let energy_stats = self.read_energy_meter(&[]);
        self.prev_reading_stw = energy_stats;
    }

    pub fn end_gc(&mut self) {
        let mut energy_stats = self.read_energy_meter(&[]);
        for i in 0..energy_stats.len() {
            energy_stats[i].energy_uW_s -= self.prev_reading_stw[i].energy_uW_s;
            energy_stats[i].duration_ms -= self.prev_reading_stw[i].duration_ms;
            self.reading_stw[i].energy_uW_s += energy_stats[i].energy_uW_s;
            self.reading_stw[i].duration_ms += energy_stats[i].duration_ms;
        }
    }

    pub fn print_column_names(&self, output_string: &mut String) {
        for channel_info in self.get_energy_meter_info() {
            output_string.push_str(
                format!(
                    "{}.total.energy\t{}.avg.power\t{}.other.energy\t{}.other.avg.power\t{}.stw.energy\t{}.stw.avg.power\t",
                    channel_info.name, channel_info.name,
                    channel_info.name, channel_info.name,
                    channel_info.name, channel_info.name,
                )
                .as_str(),
            );
        }
        output_string.push_str("total.energy\tavg.power\t");
    }

    pub fn print_stats(&self, output_string: &mut String) {
        let mut avg_power = 0_f64;
        let mut total_energy = 0;
        let mut total_duration = 0;
        for channel_info in self.get_energy_meter_info() {
            let index = self.channel_ids[&channel_info.name] as usize;
            let energy = self.reading[index].energy_uW_s - self.prev_reading[index].energy_uW_s;
            let duration = self.reading[index].duration_ms - self.prev_reading[index].duration_ms;
            let power = energy as f64 / (duration as f64 / 1000_f64);
            let stw_energy = self.reading_stw[index].energy_uW_s;
            let stw_duration = self.reading_stw[index].duration_ms;
            let stw_power = stw_energy as f64 / (stw_duration as f64 / 1000_f64);
            let other_energy = energy - stw_energy;
            let other_duration = duration - stw_duration;
            let other_power = other_energy as f64 / (other_duration as f64 / 1000_f64);
            total_energy += energy;
            total_duration += duration;
            output_string.push_str(
                format!(
                    "{}\t{:.*}\t{}\t{:.*}\t{}\t{:.*}\t",
                    energy, 2, power, other_energy, 2, other_power, stw_energy, 2, stw_power,
                )
                .as_str(),
            );
        }
        avg_power = total_energy as f64 / (total_duration as f64 / 1000_f64);
        output_string.push_str(format!("{}\t{:.*}\t", total_energy, 2, avg_power).as_str());
    }
}
