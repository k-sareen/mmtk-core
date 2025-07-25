use super::*;
use crate::util::constants::{BYTES_IN_PAGE, BYTES_IN_WORD, LOG_BITS_IN_BYTE};
use crate::util::conversions::raw_align_up;
use crate::util::heap::layout::vm_layout::BYTES_IN_CHUNK;
use crate::util::memory::{self, MmapAnnotation};
use crate::util::metadata::metadata_val_traits::*;
#[cfg(feature = "vo_bit")]
use crate::util::metadata::vo_bit::VO_BIT_SIDE_METADATA_SPEC;
use crate::util::Address;
use num_traits::FromPrimitive;
use ranges::BitByteRange;
use std::fmt;
use std::io::Result;
use std::sync::atomic::{AtomicU8, Ordering};

/// This struct stores the specification of a side metadata bit-set.
/// It is used as an input to the (inline) functions provided by the side metadata module.
///
/// Each plan or policy which uses a metadata bit-set, needs to create an instance of this struct.
///
/// For performance reasons, objects of this struct should be constants.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SideMetadataSpec {
    /// The name for this side metadata.
    pub name: &'static str,
    /// Is this side metadata global? Local metadata is used by certain spaces,
    /// while global metadata is used by all the spaces.
    pub is_global: bool,
    /// The offset for this side metadata.
    pub offset: SideMetadataOffset,
    /// Number of bits needed per region. E.g. 0 = 1 bit, 1 = 2 bit.
    pub log_num_of_bits: usize,
    /// Number of bytes of the region. E.g. 3 = 8 bytes, 12 = 4096 bytes (page).
    pub log_bytes_in_region: usize,
}

impl SideMetadataSpec {
    /// Is this spec using contiguous side metadata? If not, it uses chunked side metadata.
    pub const fn uses_contiguous_side_metadata(&self) -> bool {
        self.is_global || cfg!(target_pointer_width = "64")
    }

    /// Is offset for this spec Address?
    pub const fn is_absolute_offset(&self) -> bool {
        self.uses_contiguous_side_metadata()
    }

    /// If offset for this spec relative? (chunked side metadata for local specs in 32 bits)
    pub const fn is_rel_offset(&self) -> bool {
        !self.is_absolute_offset()
    }

    /// Get the absolute offset for the spec.
    pub const fn get_absolute_offset(&self) -> Address {
        debug_assert!(self.is_absolute_offset());
        unsafe { self.offset.addr }
    }

    /// Get the relative offset for the spec.
    pub const fn get_rel_offset(&self) -> usize {
        debug_assert!(self.is_rel_offset());
        unsafe { self.offset.rel_offset }
    }

    /// Return the upperbound offset for the side metadata. The next side metadata should be laid out at this offset.
    #[cfg(target_pointer_width = "64")]
    pub const fn upper_bound_offset(&self) -> SideMetadataOffset {
        debug_assert!(self.is_absolute_offset());
        SideMetadataOffset {
            addr: unsafe { self.offset.addr }
                .add(crate::util::metadata::side_metadata::metadata_address_range_size(self)),
        }
    }

    /// Return the upperbound offset for the side metadata. The next side metadata should be laid out at this offset.
    #[cfg(target_pointer_width = "32")]
    pub const fn upper_bound_offset(&self) -> SideMetadataOffset {
        if self.is_absolute_offset() {
            SideMetadataOffset {
                addr: unsafe { self.offset.addr }
                    .add(crate::util::metadata::side_metadata::metadata_address_range_size(self)),
            }
        } else {
            SideMetadataOffset {
                rel_offset: unsafe { self.offset.rel_offset }
                    + crate::util::metadata::side_metadata::metadata_bytes_per_chunk(
                        self.log_bytes_in_region,
                        self.log_num_of_bits,
                    ),
            }
        }
    }

    /// The upper bound address for metadata address computed for this global spec. The computed metadata address
    /// should never be larger than this address. Otherwise, we are accessing the metadata that is laid out
    /// after this spec. This spec must be a contiguous side metadata spec (which uses address
    /// as offset).
    pub const fn upper_bound_address_for_contiguous(&self) -> Address {
        debug_assert!(self.is_absolute_offset());
        unsafe { self.upper_bound_offset().addr }
    }

    /// The upper bound address for metadata address computed for this global spec. The computed metadata address
    /// should never be larger than this address. Otherwise, we are accessing the metadata that is laid out
    /// after this spec. This spec must be a chunked side metadata spec (which uses relative offset). Only 32 bit local
    /// side metadata uses chunked metadata.
    #[cfg(target_pointer_width = "32")]
    pub const fn upper_bound_address_for_chunked(&self, data_addr: Address) -> Address {
        debug_assert!(self.is_rel_offset());
        address_to_meta_chunk_addr(data_addr).add(unsafe { self.upper_bound_offset().rel_offset })
    }

    /// Used only for debugging.
    /// This panics if the required metadata is not mapped
    #[cfg(debug_assertions)]
    pub(crate) fn assert_metadata_mapped(&self, data_addr: Address) {
        let meta_start = address_to_meta_address(self, data_addr).align_down(BYTES_IN_PAGE);

        trace!(
            "ensure_metadata_is_mapped({}).meta_start({})",
            data_addr,
            meta_start
        );

        memory::panic_if_unmapped(
            meta_start,
            BYTES_IN_PAGE,
            &MmapAnnotation::Misc {
                name: "assert_metadata_mapped",
            },
        );
    }

    #[cfg(debug_assertions)]
    pub(crate) fn are_different_metadata_bits(&self, addr1: Address, addr2: Address) -> bool {
        let a1 = address_to_meta_address(self, addr1);
        let a2 = address_to_meta_address(self, addr2);
        let s1 = meta_byte_lshift(self, addr1);
        let s2 = meta_byte_lshift(self, addr2);
        (a1, s1) != (a2, s2)
    }

    /// Used only for debugging.
    /// * Assert if the given MetadataValue type matches the spec.
    /// * Assert if the provided value is valid in the spec.
    #[cfg(debug_assertions)]
    fn assert_value_type<T: MetadataValue>(&self, val: Option<T>) {
        let log_b = self.log_num_of_bits;
        match log_b {
            _ if log_b < 3 => {
                assert_eq!(T::LOG2, 3);
                if let Some(v) = val {
                    assert!(
                        v.to_u8().unwrap() < (1 << (1 << log_b)),
                        "Input value {:?} is invalid for the spec {:?}",
                        v,
                        self
                    );
                }
            }
            3..=6 => assert_eq!(T::LOG2, log_b as u32),
            _ => unreachable!("side metadata > {}-bits is not supported", 1 << log_b),
        }
    }

    /// Check with the mmapper to see if side metadata is mapped for the spec for the data address.
    pub(crate) fn is_mapped(&self, data_addr: Address) -> bool {
        use crate::MMAPPER;
        let meta_addr = address_to_meta_address(self, data_addr);
        MMAPPER.is_mapped_address(meta_addr)
    }

    /// This method is used for bulk zeroing side metadata for a data address range.
    pub(crate) fn zero_meta_bits(
        meta_start_addr: Address,
        meta_start_bit: u8,
        meta_end_addr: Address,
        meta_end_bit: u8,
    ) {
        let mut visitor = |range| {
            match range {
                BitByteRange::Bytes { start, end } => {
                    memory::zero(start, end - start);
                    false
                }
                BitByteRange::BitsInByte {
                    addr,
                    bit_start,
                    bit_end,
                } => {
                    // we are zeroing selected bit in one byte
                    // Get a mask that the bits we need to zero are set to zero, and the other bits are 1.
                    let mask: u8 =
                        u8::MAX.checked_shl(bit_end as u32).unwrap_or(0) | !(u8::MAX << bit_start);
                    unsafe { addr.as_ref::<AtomicU8>() }.fetch_and(mask, Ordering::SeqCst);
                    false
                }
            }
        };
        ranges::break_bit_range(
            meta_start_addr,
            meta_start_bit,
            meta_end_addr,
            meta_end_bit,
            true,
            &mut visitor,
        );
    }

    /// This method is used for bulk setting side metadata for a data address range.
    pub(crate) fn set_meta_bits(
        meta_start_addr: Address,
        meta_start_bit: u8,
        meta_end_addr: Address,
        meta_end_bit: u8,
    ) {
        let mut visitor = |range| {
            match range {
                BitByteRange::Bytes { start, end } => {
                    memory::set(start, 0xff, end - start);
                    false
                }
                BitByteRange::BitsInByte {
                    addr,
                    bit_start,
                    bit_end,
                } => {
                    // we are setting selected bits in one byte
                    // Get a mask that the bits we need to set are 1, and the other bits are 0.
                    let mask: u8 = !(u8::MAX.checked_shl(bit_end as u32).unwrap_or(0))
                        & (u8::MAX << bit_start);
                    unsafe { addr.as_ref::<AtomicU8>() }.fetch_or(mask, Ordering::SeqCst);
                    false
                }
            }
        };
        ranges::break_bit_range(
            meta_start_addr,
            meta_start_bit,
            meta_end_addr,
            meta_end_bit,
            true,
            &mut visitor,
        );
    }

    /// This method does bulk update for the given data range. It calculates the metadata bits for the given data range,
    /// and invoke the given method to update the metadata bits.
    pub(super) fn bulk_update_metadata(
        &self,
        start: Address,
        size: usize,
        update_meta_bits: &impl Fn(Address, u8, Address, u8),
    ) {
        // Update bits for a contiguous side metadata spec. We can simply calculate the data end address, and
        // calculate the metadata address for the data end.
        let update_contiguous = |data_start: Address, data_bytes: usize| {
            if data_bytes == 0 {
                return;
            }
            let meta_start = address_to_meta_address(self, data_start);
            let meta_start_shift = meta_byte_lshift(self, data_start);
            let meta_end = address_to_meta_address(self, data_start + data_bytes);
            let meta_end_shift = meta_byte_lshift(self, data_start + data_bytes);
            update_meta_bits(meta_start, meta_start_shift, meta_end, meta_end_shift);
        };

        // Update bits for a discontiguous side metadata spec (chunked metadata). The side metadata for different
        // chunks are stored in discontiguous memory. For example, Chunk #2 follows Chunk #1, but the side metadata
        // for Chunk #2 does not immediately follow the side metadata for Chunk #1. So when we bulk update metadata for Chunk #1,
        // we cannot update up to the metadata address for the Chunk #2 start. Otherwise it may modify unrelated metadata
        // between the two chunks' metadata.
        // Instead, we compute how many bytes/bits we need to update.
        // The data for which the metadata will be updates has to be in the same chunk.
        #[cfg(target_pointer_width = "32")]
        let update_discontiguous = |data_start: Address, data_bytes: usize| {
            use crate::util::constants::BITS_IN_BYTE;
            if data_bytes == 0 {
                return;
            }
            debug_assert_eq!(
                data_start.align_down(BYTES_IN_CHUNK),
                (data_start + data_bytes - 1).align_down(BYTES_IN_CHUNK),
                "The data to be zeroed in discontiguous specs needs to be in the same chunk"
            );
            let meta_start = address_to_meta_address(self, data_start);
            let meta_start_shift = meta_byte_lshift(self, data_start);
            // How many bits we need to zero for data_bytes
            let meta_total_bits = (data_bytes >> self.log_bytes_in_region) << self.log_num_of_bits;
            let meta_delta_bytes = meta_total_bits >> LOG_BITS_IN_BYTE;
            let meta_delta_bits: u8 = (meta_total_bits % BITS_IN_BYTE) as u8;
            // Calculate the end byte/addr and end bit
            let (meta_end, meta_end_shift) = {
                let mut end_addr = meta_start + meta_delta_bytes;
                let mut end_bit = meta_start_shift + meta_delta_bits;
                if end_bit >= BITS_IN_BYTE as u8 {
                    end_bit -= BITS_IN_BYTE as u8;
                    end_addr += 1usize;
                }
                (end_addr, end_bit)
            };

            update_meta_bits(meta_start, meta_start_shift, meta_end, meta_end_shift);
        };

        if cfg!(target_pointer_width = "64") || self.is_global {
            update_contiguous(start, size);
        }
        #[cfg(target_pointer_width = "32")]
        if !self.is_global {
            // per chunk policy-specific metadata for 32-bits targets
            let chunk_num = ((start + size).align_down(BYTES_IN_CHUNK)
                - start.align_down(BYTES_IN_CHUNK))
                / BYTES_IN_CHUNK;
            if chunk_num == 0 {
                update_discontiguous(start, size);
            } else {
                let second_data_chunk = start.align_up(BYTES_IN_CHUNK);
                // bzero the first sub-chunk
                update_discontiguous(start, second_data_chunk - start);

                let last_data_chunk = (start + size).align_down(BYTES_IN_CHUNK);
                // bzero the last sub-chunk
                update_discontiguous(last_data_chunk, start + size - last_data_chunk);
                let mut next_data_chunk = second_data_chunk;

                // bzero all chunks in the middle
                while next_data_chunk != last_data_chunk {
                    update_discontiguous(next_data_chunk, BYTES_IN_CHUNK);
                    next_data_chunk += BYTES_IN_CHUNK;
                }
            }
        }
    }

    /// Bulk-zero a specific metadata for a memory region. Note that this method is more sophisiticated than a simple memset, especially in the following
    /// cases:
    /// * the metadata for the range includes partial bytes (a few bits in the same byte).
    /// * for 32 bits local side metadata, the side metadata is stored in discontiguous chunks, we will have to bulk zero for each chunk's side metadata.
    ///
    /// # Arguments
    ///
    /// * `start`: The starting address of a memory region. The side metadata starting from this data address will be zeroed.
    /// * `size`: The size of the memory region.
    pub fn bzero_metadata(&self, start: Address, size: usize) {
        #[cfg(feature = "extreme_assertions")]
        let _lock = sanity::SANITY_LOCK.lock().unwrap();

        #[cfg(feature = "extreme_assertions")]
        sanity::verify_bzero(self, start, size);

        self.bulk_update_metadata(start, size, &Self::zero_meta_bits)
    }

    /// Bulk set a specific metadata for a memory region. Note that this method is more sophisiticated than a simple memset, especially in the following
    /// cases:
    /// * the metadata for the range includes partial bytes (a few bits in the same byte).
    /// * for 32 bits local side metadata, the side metadata is stored in discontiguous chunks, we will have to bulk set for each chunk's side metadata.
    ///
    /// # Arguments
    ///
    /// * `start`: The starting address of a memory region. The side metadata starting from this data address will be set to all 1s in the bits.
    /// * `size`: The size of the memory region.
    pub fn bset_metadata(&self, start: Address, size: usize) {
        #[cfg(feature = "extreme_assertions")]
        let _lock = sanity::SANITY_LOCK.lock().unwrap();

        #[cfg(feature = "extreme_assertions")]
        sanity::verify_bset(self, start, size);

        self.bulk_update_metadata(start, size, &Self::set_meta_bits)
    }

    /// Bulk copy the `other` side metadata for a memory region to this side metadata.
    ///
    /// This function only works for contiguous metadata.
    /// Curently all global metadata are contiguous.
    /// It also requires the other metadata to have the same number of bits per region
    /// and the same region size.
    ///
    /// # Arguments
    ///
    /// * `start`: The starting address of a memory region.
    /// * `size`: The size of the memory region.
    /// * `other`: The other metadata to copy from.
    pub fn bcopy_metadata_contiguous(&self, start: Address, size: usize, other: &SideMetadataSpec) {
        #[cfg(feature = "extreme_assertions")]
        let _lock = sanity::SANITY_LOCK.lock().unwrap();

        #[cfg(feature = "extreme_assertions")]
        sanity::verify_bcopy(self, start, size, other);

        debug_assert_eq!(other.log_bytes_in_region, self.log_bytes_in_region);
        debug_assert_eq!(other.log_num_of_bits, self.log_num_of_bits);

        let dst_meta_start_addr = address_to_meta_address(self, start);
        let dst_meta_start_bit = meta_byte_lshift(self, start);
        let dst_meta_end_addr = address_to_meta_address(self, start + size);
        let dst_meta_end_bit = meta_byte_lshift(self, start + size);

        let src_meta_start_addr = address_to_meta_address(other, start);
        let src_meta_start_bit = meta_byte_lshift(other, start);

        debug_assert_eq!(dst_meta_start_bit, src_meta_start_bit);

        let mut visitor = |range| {
            match range {
                BitByteRange::Bytes {
                    start: dst_start,
                    end: dst_end,
                } => unsafe {
                    let byte_offset = dst_start - dst_meta_start_addr;
                    let src_start = src_meta_start_addr + byte_offset;
                    let size = dst_end - dst_start;
                    std::ptr::copy::<u8>(src_start.to_ptr(), dst_start.to_mut_ptr(), size);
                    false
                },
                BitByteRange::BitsInByte {
                    addr: dst,
                    bit_start,
                    bit_end,
                } => {
                    let byte_offset = dst - dst_meta_start_addr;
                    let src = src_meta_start_addr + byte_offset;
                    // we are setting selected bits in one byte
                    let mask: u8 = !(u8::MAX.checked_shl(bit_end as u32).unwrap_or(0))
                        & (u8::MAX << bit_start); // Get a mask that the bits we need to set are 1, and the other bits are 0.
                    let old_src = unsafe { src.as_ref::<AtomicU8>() }.load(Ordering::Relaxed);
                    let old_dst = unsafe { dst.as_ref::<AtomicU8>() }.load(Ordering::Relaxed);
                    let new = (old_src & mask) | (old_dst & !mask);
                    unsafe { dst.as_ref::<AtomicU8>() }.store(new, Ordering::Relaxed);
                    false
                }
            }
        };

        ranges::break_bit_range(
            dst_meta_start_addr,
            dst_meta_start_bit,
            dst_meta_end_addr,
            dst_meta_end_bit,
            true,
            &mut visitor,
        );
    }

    /// This is a wrapper method for implementing side metadata access. It does nothing other than
    /// calling the access function with no overhead, but in debug builds,
    /// it includes multiple checks to make sure the access is sane.
    /// * check whether the given value type matches the number of bits for the side metadata.
    /// * check if the side metadata memory is mapped.
    /// * check if the side metadata content is correct based on a sanity map (only for extreme assertions).
    #[allow(unused_variables)] // data_addr/input is not used in release build
    fn side_metadata_access<
        const CHECK_VALUE: bool,
        T: MetadataValue,
        R: Copy,
        F: FnOnce() -> R,
        V: FnOnce(R),
    >(
        &self,
        data_addr: Address,
        input: Option<T>,
        access_func: F,
        verify_func: V,
    ) -> R {
        // With extreme assertions, we maintain a sanity table for each side metadata access. For whatever we store in
        // side metadata, we store in the sanity table. So we can use that table to check if its results are conssitent
        // with the actual side metadata.
        // To achieve this, we need to apply a lock when we access side metadata. This will hide some concurrency bugs,
        // but makes it possible for us to assert our side metadata implementation is correct.
        #[cfg(feature = "extreme_assertions")]
        let _lock = sanity::SANITY_LOCK.lock().unwrap();

        // A few checks
        #[cfg(debug_assertions)]
        {
            if CHECK_VALUE {
                self.assert_value_type::<T>(input);
            }
            #[cfg(feature = "extreme_assertions")]
            self.assert_metadata_mapped(data_addr);
        }

        // Actual access to the side metadata
        let ret = access_func();

        // Verifying the side metadata: checks the result with the sanity table, or store some results to the sanity table
        if CHECK_VALUE {
            verify_func(ret);
        }

        ret
    }

    /// Non-atomic load of metadata.
    ///
    /// # Safety
    ///
    /// This is unsafe because:
    ///
    /// 1. Concurrent access to this operation is undefined behaviour.
    /// 2. Interleaving Non-atomic and atomic operations is undefined behaviour.
    pub unsafe fn load<T: MetadataValue>(&self, data_addr: Address) -> T {
        self.side_metadata_access::<true, T, _, _, _>(
            data_addr,
            None,
            || {
                let meta_addr = address_to_meta_address(self, data_addr);
                let bits_num_log = self.log_num_of_bits;
                if bits_num_log < 3 {
                    let lshift = meta_byte_lshift(self, data_addr);
                    let mask = meta_byte_mask(self) << lshift;
                    let byte_val = meta_addr.load::<u8>();

                    FromPrimitive::from_u8((byte_val & mask) >> lshift).unwrap()
                } else {
                    meta_addr.load::<T>()
                }
            },
            |_v| {
                #[cfg(feature = "extreme_assertions")]
                sanity::verify_load(self, data_addr, _v);
            },
        )
    }

    /// Non-atomic store of metadata.
    ///
    /// # Safety
    ///
    /// This is unsafe because:
    ///
    /// 1. Concurrent access to this operation is undefined behaviour.
    /// 2. Interleaving Non-atomic and atomic operations is undefined behaviour.
    pub unsafe fn store<T: MetadataValue>(&self, data_addr: Address, metadata: T) {
        self.side_metadata_access::<true, T, _, _, _>(
            data_addr,
            Some(metadata),
            || {
                let meta_addr = address_to_meta_address(self, data_addr);
                let bits_num_log = self.log_num_of_bits;
                if bits_num_log < 3 {
                    let lshift = meta_byte_lshift(self, data_addr);
                    let mask = meta_byte_mask(self) << lshift;
                    let old_val = meta_addr.load::<u8>();
                    let new_val = (old_val & !mask) | (metadata.to_u8().unwrap() << lshift);

                    meta_addr.store::<u8>(new_val);
                } else {
                    meta_addr.store::<T>(metadata);
                }
            },
            |_| {
                #[cfg(feature = "extreme_assertions")]
                sanity::verify_store(self, data_addr, metadata);
            },
        )
    }

    /// Loads a value from the side metadata for the given address.
    /// This method has similar semantics to `store` in Rust atomics.
    pub fn load_atomic<T: MetadataValue>(&self, data_addr: Address, order: Ordering) -> T {
        self.side_metadata_access::<true, T, _, _, _>(
            data_addr,
            None,
            || {
                let meta_addr = address_to_meta_address(self, data_addr);
                let bits_num_log = self.log_num_of_bits;
                if bits_num_log < 3 {
                    let lshift = meta_byte_lshift(self, data_addr);
                    let mask = meta_byte_mask(self) << lshift;
                    let byte_val = unsafe { meta_addr.atomic_load::<AtomicU8>(order) };
                    FromPrimitive::from_u8((byte_val & mask) >> lshift).unwrap()
                } else {
                    unsafe { T::load_atomic(meta_addr, order) }
                }
            },
            |_v| {
                #[cfg(feature = "extreme_assertions")]
                sanity::verify_load(self, data_addr, _v);
            },
        )
    }

    /// Store the given value to the side metadata for the given address.
    /// This method has similar semantics to `store` in Rust atomics.
    pub fn store_atomic<T: MetadataValue>(&self, data_addr: Address, metadata: T, order: Ordering) {
        self.side_metadata_access::<true, T, _, _, _>(
            data_addr,
            Some(metadata),
            || {
                let meta_addr = address_to_meta_address(self, data_addr);
                let bits_num_log = self.log_num_of_bits;
                if bits_num_log < 3 {
                    let lshift = meta_byte_lshift(self, data_addr);
                    let mask = meta_byte_mask(self) << lshift;
                    let metadata_u8 = metadata.to_u8().unwrap();
                    let _ = unsafe {
                        <u8 as MetadataValue>::fetch_update(meta_addr, order, order, |v: u8| {
                            Some((v & !mask) | (metadata_u8 << lshift))
                        })
                    };
                } else {
                    unsafe {
                        T::store_atomic(meta_addr, metadata, order);
                    }
                }
            },
            |_| {
                #[cfg(feature = "extreme_assertions")]
                sanity::verify_store(self, data_addr, metadata);
            },
        )
    }

    /// Non-atomically store zero to the side metadata for the given address.
    /// This method mainly facilitates clearing multiple metadata specs for the same address in a loop.
    ///
    /// # Safety
    ///
    /// This is unsafe because:
    ///
    /// 1. Concurrent access to this operation is undefined behaviour.
    /// 2. Interleaving Non-atomic and atomic operations is undefined behaviour.
    pub unsafe fn set_zero(&self, data_addr: Address) {
        use num_traits::Zero;
        match self.log_num_of_bits {
            0..=3 => self.store(data_addr, u8::zero()),
            4 => self.store(data_addr, u16::zero()),
            5 => self.store(data_addr, u32::zero()),
            6 => self.store(data_addr, u64::zero()),
            _ => unreachable!(),
        }
    }

    /// Atomiccally store zero to the side metadata for the given address.
    /// This method mainly facilitates clearing multiple metadata specs for the same address in a loop.
    pub fn set_zero_atomic(&self, data_addr: Address, order: Ordering) {
        use num_traits::Zero;
        match self.log_num_of_bits {
            0..=3 => self.store_atomic(data_addr, u8::zero(), order),
            4 => self.store_atomic(data_addr, u16::zero(), order),
            5 => self.store_atomic(data_addr, u32::zero(), order),
            6 => self.store_atomic(data_addr, u64::zero(), order),
            _ => unreachable!(),
        }
    }

    /// Atomically store one to the side metadata for the data address with the _possible_ side effect of corrupting
    /// and setting the entire byte in the side metadata to 0xff. This can only be used for side metadata smaller
    /// than a byte.
    /// This means it does not only set the side metadata for the data address, and it may also have a side effect of
    /// corrupting and setting the side metadata for the adjacent data addresses. This method is only intended to be
    /// used as an optimization to skip masking and setting bits in some scenarios where setting adjancent bits to 1 is benign.
    ///
    /// # Safety
    /// This method _may_ corrupt and set adjacent bits in the side metadata as a side effect. The user must
    /// make sure that this behavior is correct and must not rely on the side effect of this method to set bits.
    pub unsafe fn set_raw_byte_atomic(&self, data_addr: Address, order: Ordering) {
        debug_assert!(self.log_num_of_bits < 3);
        cfg_if::cfg_if! {
            if #[cfg(feature = "extreme_assertions")] {
                // For extreme assertions, we only set 1 to the given address.
                self.store_atomic::<u8>(data_addr, 1, order)
            } else {
                self.side_metadata_access::<false, u8, _, _, _>(
                    data_addr,
                    Some(1u8),
                    || {
                        let meta_addr = address_to_meta_address(self, data_addr);
                        u8::store_atomic(meta_addr, 0xffu8, order);
                    },
                    |_| {}
                )
            }
        }
    }

    /// Load the raw byte in the side metadata byte that is mapped to the data address.
    ///
    /// # Safety
    /// This is unsafe because:
    ///
    /// 1. Concurrent access to this operation is undefined behaviour.
    /// 2. Interleaving Non-atomic and atomic operations is undefined behaviour.
    pub unsafe fn load_raw_byte(&self, data_addr: Address) -> u8 {
        debug_assert!(self.log_num_of_bits < 3);
        self.side_metadata_access::<false, u8, _, _, _>(
            data_addr,
            None,
            || {
                let meta_addr = address_to_meta_address(self, data_addr);
                meta_addr.load::<u8>()
            },
            |_| {},
        )
    }

    /// Load the raw word that includes the side metadata byte mapped to the data address.
    ///
    /// # Safety
    /// This is unsafe because:
    ///
    /// 1. Concurrent access to this operation is undefined behaviour.
    /// 2. Interleaving Non-atomic and atomic operations is undefined behaviour.
    pub unsafe fn load_raw_word(&self, data_addr: Address) -> usize {
        use crate::util::constants::*;
        debug_assert!(self.log_num_of_bits < (LOG_BITS_IN_BYTE + LOG_BYTES_IN_ADDRESS) as usize);
        self.side_metadata_access::<false, usize, _, _, _>(
            data_addr,
            None,
            || {
                let meta_addr = address_to_meta_address(self, data_addr);
                let aligned_meta_addr = meta_addr.align_down(BYTES_IN_ADDRESS);
                aligned_meta_addr.load::<usize>()
            },
            |_| {},
        )
    }

    /// Stores the new value into the side metadata for the gien address if the current value is the same as the old value.
    /// This method has similar semantics to `compare_exchange` in Rust atomics.
    /// The return value is a result indicating whether the new value was written and containing the previous value.
    /// On success this value is guaranteed to be equal to current.
    pub fn compare_exchange_atomic<T: MetadataValue>(
        &self,
        data_addr: Address,
        old_metadata: T,
        new_metadata: T,
        success_order: Ordering,
        failure_order: Ordering,
    ) -> std::result::Result<T, T> {
        self.side_metadata_access::<true, T, _, _, _>(
            data_addr,
            Some(new_metadata),
            || {
                let meta_addr = address_to_meta_address(self, data_addr);
                let bits_num_log = self.log_num_of_bits;
                if bits_num_log < 3 {
                    let lshift = meta_byte_lshift(self, data_addr);
                    let mask = meta_byte_mask(self) << lshift;

                    let real_old_byte = unsafe { meta_addr.atomic_load::<AtomicU8>(success_order) };
                    let expected_old_byte =
                        (real_old_byte & !mask) | ((old_metadata.to_u8().unwrap()) << lshift);
                    let expected_new_byte =
                        (expected_old_byte & !mask) | ((new_metadata.to_u8().unwrap()) << lshift);

                    unsafe {
                        meta_addr.compare_exchange::<AtomicU8>(
                            expected_old_byte,
                            expected_new_byte,
                            success_order,
                            failure_order,
                        )
                    }
                    .map(|x| FromPrimitive::from_u8((x & mask) >> lshift).unwrap())
                    .map_err(|x| FromPrimitive::from_u8((x & mask) >> lshift).unwrap())
                } else {
                    unsafe {
                        T::compare_exchange(
                            meta_addr,
                            old_metadata,
                            new_metadata,
                            success_order,
                            failure_order,
                        )
                    }
                }
            },
            |_res| {
                #[cfg(feature = "extreme_assertions")]
                if _res.is_ok() {
                    sanity::verify_store(self, data_addr, new_metadata);
                }
            },
        )
    }

    /// This is used to implement fetch_add/sub for bits.
    /// For fetch_and/or, we don't necessarily need this method. We could directly do fetch_and/or on the u8.
    fn fetch_ops_on_bits<F: Fn(u8) -> u8>(
        &self,
        data_addr: Address,
        meta_addr: Address,
        set_order: Ordering,
        fetch_order: Ordering,
        update: F,
    ) -> u8 {
        let lshift = meta_byte_lshift(self, data_addr);
        let mask = meta_byte_mask(self) << lshift;

        let old_raw_byte = unsafe {
            <u8 as MetadataValue>::fetch_update(
                meta_addr,
                set_order,
                fetch_order,
                |raw_byte: u8| {
                    let old_val = (raw_byte & mask) >> lshift;
                    let new_val = update(old_val);
                    let new_raw_byte = (raw_byte & !mask) | ((new_val << lshift) & mask);
                    Some(new_raw_byte)
                },
            )
        }
        .unwrap();
        (old_raw_byte & mask) >> lshift
    }

    /// Adds the value to the current value for this side metadata for the given address.
    /// This method has similar semantics to `fetch_add` in Rust atomics.
    /// Returns the previous value.
    pub fn fetch_add_atomic<T: MetadataValue>(
        &self,
        data_addr: Address,
        val: T,
        order: Ordering,
    ) -> T {
        self.side_metadata_access::<true, T, _, _, _>(
            data_addr,
            Some(val),
            || {
                let meta_addr = address_to_meta_address(self, data_addr);
                let bits_num_log = self.log_num_of_bits;
                if bits_num_log < 3 {
                    FromPrimitive::from_u8(self.fetch_ops_on_bits(
                        data_addr,
                        meta_addr,
                        order,
                        order,
                        |x: u8| x.wrapping_add(val.to_u8().unwrap()),
                    ))
                    .unwrap()
                } else {
                    unsafe { T::fetch_add(meta_addr, val, order) }
                }
            },
            |_old_val| {
                #[cfg(feature = "extreme_assertions")]
                sanity::verify_update::<T>(self, data_addr, _old_val, _old_val.wrapping_add(&val))
            },
        )
    }

    /// Subtracts the value from the current value for this side metadata for the given address.
    /// This method has similar semantics to `fetch_sub` in Rust atomics.
    /// Returns the previous value.
    pub fn fetch_sub_atomic<T: MetadataValue>(
        &self,
        data_addr: Address,
        val: T,
        order: Ordering,
    ) -> T {
        self.side_metadata_access::<true, T, _, _, _>(
            data_addr,
            Some(val),
            || {
                let meta_addr = address_to_meta_address(self, data_addr);
                if self.log_num_of_bits < 3 {
                    FromPrimitive::from_u8(self.fetch_ops_on_bits(
                        data_addr,
                        meta_addr,
                        order,
                        order,
                        |x: u8| x.wrapping_sub(val.to_u8().unwrap()),
                    ))
                    .unwrap()
                } else {
                    unsafe { T::fetch_sub(meta_addr, val, order) }
                }
            },
            |_old_val| {
                #[cfg(feature = "extreme_assertions")]
                sanity::verify_update::<T>(self, data_addr, _old_val, _old_val.wrapping_sub(&val))
            },
        )
    }

    /// Bitwise 'and' the value with the current value for this side metadata for the given address.
    /// This method has similar semantics to `fetch_and` in Rust atomics.
    /// Returns the previous value.
    pub fn fetch_and_atomic<T: MetadataValue>(
        &self,
        data_addr: Address,
        val: T,
        order: Ordering,
    ) -> T {
        self.side_metadata_access::<true, T, _, _, _>(
            data_addr,
            Some(val),
            || {
                let meta_addr = address_to_meta_address(self, data_addr);
                if self.log_num_of_bits < 3 {
                    let lshift = meta_byte_lshift(self, data_addr);
                    let mask = meta_byte_mask(self) << lshift;
                    // We do not need to use fetch_ops_on_bits(), we can just set irrelavent bits to 1, and do fetch_and
                    let rhs = (val.to_u8().unwrap() << lshift) | !mask;
                    let old_raw_byte =
                        unsafe { <u8 as MetadataValue>::fetch_and(meta_addr, rhs, order) };
                    let old_val = (old_raw_byte & mask) >> lshift;
                    FromPrimitive::from_u8(old_val).unwrap()
                } else {
                    unsafe { T::fetch_and(meta_addr, val, order) }
                }
            },
            |_old_val| {
                #[cfg(feature = "extreme_assertions")]
                sanity::verify_update::<T>(self, data_addr, _old_val, _old_val.bitand(val))
            },
        )
    }

    /// Bitwise 'or' the value with the current value for this side metadata for the given address.
    /// This method has similar semantics to `fetch_or` in Rust atomics.
    /// Returns the previous value.
    pub fn fetch_or_atomic<T: MetadataValue>(
        &self,
        data_addr: Address,
        val: T,
        order: Ordering,
    ) -> T {
        self.side_metadata_access::<true, T, _, _, _>(
            data_addr,
            Some(val),
            || {
                let meta_addr = address_to_meta_address(self, data_addr);
                if self.log_num_of_bits < 3 {
                    let lshift = meta_byte_lshift(self, data_addr);
                    let mask = meta_byte_mask(self) << lshift;
                    // We do not need to use fetch_ops_on_bits(), we can just set irrelavent bits to 0, and do fetch_or
                    let rhs = (val.to_u8().unwrap() << lshift) & mask;
                    let old_raw_byte =
                        unsafe { <u8 as MetadataValue>::fetch_or(meta_addr, rhs, order) };
                    let old_val = (old_raw_byte & mask) >> lshift;
                    FromPrimitive::from_u8(old_val).unwrap()
                } else {
                    unsafe { T::fetch_or(meta_addr, val, order) }
                }
            },
            |_old_val| {
                #[cfg(feature = "extreme_assertions")]
                sanity::verify_update::<T>(self, data_addr, _old_val, _old_val.bitor(val))
            },
        )
    }

    /// Fetches the value for this side metadata for the given address, and applies a function to it that returns an optional new value.
    /// This method has similar semantics to `fetch_update` in Rust atomics.
    /// Returns a Result of Ok(previous_value) if the function returned Some(_), else Err(previous_value).
    pub fn fetch_update_atomic<T: MetadataValue, F: FnMut(T) -> Option<T> + Copy>(
        &self,
        data_addr: Address,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: F,
    ) -> std::result::Result<T, T> {
        self.side_metadata_access::<true, T, _, _, _>(
            data_addr,
            None,
            move || -> std::result::Result<T, T> {
                let meta_addr = address_to_meta_address(self, data_addr);
                if self.log_num_of_bits < 3 {
                    let lshift = meta_byte_lshift(self, data_addr);
                    let mask = meta_byte_mask(self) << lshift;

                    unsafe {
                        <u8 as MetadataValue>::fetch_update(
                            meta_addr,
                            set_order,
                            fetch_order,
                            |raw_byte: u8| {
                                let old_val = (raw_byte & mask) >> lshift;
                                f(FromPrimitive::from_u8(old_val).unwrap()).map(|new_val| {
                                    (raw_byte & !mask)
                                        | ((new_val.to_u8().unwrap() << lshift) & mask)
                                })
                            },
                        )
                    }
                    .map(|x| FromPrimitive::from_u8((x & mask) >> lshift).unwrap())
                    .map_err(|x| FromPrimitive::from_u8((x & mask) >> lshift).unwrap())
                } else {
                    unsafe { T::fetch_update(meta_addr, set_order, fetch_order, f) }
                }
            },
            |_result| {
                #[cfg(feature = "extreme_assertions")]
                if let Ok(old_val) = _result {
                    sanity::verify_update::<T>(self, data_addr, old_val, f(old_val).unwrap())
                }
            },
        )
    }

    /// Search for a data address that has a non zero value in the side metadata. The search starts from the given data address (including this address),
    /// and iterates backwards for the given bytes (non inclusive) before the data address.
    ///
    /// The data_addr and the corresponding side metadata address may not be mapped. Thus when this function checks the given data address, and
    /// when it searches back, it needs to check if the address is mapped or not to avoid loading from an unmapped address.
    ///
    /// This function returns an address that is aligned to the region of this side metadata (`log_bytes_per_region`), and the side metadata
    /// for the address is non zero.
    ///
    /// # Safety
    ///
    /// This function uses non-atomic load for the side metadata. The user needs to make sure
    /// that there is no other thread that is mutating the side metadata.
    #[allow(clippy::let_and_return)]
    pub unsafe fn find_prev_non_zero_value<T: MetadataValue>(
        &self,
        data_addr: Address,
        search_limit_bytes: usize,
    ) -> Option<Address> {
        debug_assert!(search_limit_bytes > 0);

        if self.uses_contiguous_side_metadata() {
            // Contiguous side metadata
            let result = self.find_prev_non_zero_value_fast::<T>(data_addr, search_limit_bytes);
            #[cfg(debug_assertions)]
            {
                // Double check if the implementation is correct
                let result2 =
                    self.find_prev_non_zero_value_simple::<T>(data_addr, search_limit_bytes);
                assert_eq!(result, result2, "find_prev_non_zero_value_fast returned a diffrent result from the naive implementation.");
            }
            result
        } else {
            // TODO: We should be able to optimize further for this case. However, we need to be careful that the side metadata
            // is not contiguous, and we need to skip to the next chunk's side metadata when we search to a different chunk.
            // This won't be used for VO bit, as VO bit is global and is always contiguous. So for now, I am not bothered to do it.
            warn!("We are trying to search non zero bits in an discontiguous side metadata. The performance is slow, as MMTk does not optimize for this case.");
            self.find_prev_non_zero_value_simple::<T>(data_addr, search_limit_bytes)
        }
    }

    fn find_prev_non_zero_value_simple<T: MetadataValue>(
        &self,
        data_addr: Address,
        search_limit_bytes: usize,
    ) -> Option<Address> {
        let region_bytes = 1 << self.log_bytes_in_region;
        // Figure out the range that we need to search.
        let start_addr = data_addr.align_down(region_bytes);
        let end_addr = data_addr.saturating_sub(search_limit_bytes) + 1usize;

        let mut cursor = start_addr;
        while cursor >= end_addr {
            // We encounter an unmapped address. Just return None.
            if !cursor.is_mapped() {
                return None;
            }
            // If we find non-zero value, just return it.
            if !unsafe { self.load::<T>(cursor).is_zero() } {
                return Some(cursor);
            }
            cursor -= region_bytes;
        }
        None
    }

    #[allow(clippy::let_and_return)]
    fn find_prev_non_zero_value_fast<T: MetadataValue>(
        &self,
        data_addr: Address,
        search_limit_bytes: usize,
    ) -> Option<Address> {
        debug_assert!(self.uses_contiguous_side_metadata());

        // Quick check if the data address is mapped at all.
        if !data_addr.is_mapped() {
            return None;
        }
        // Quick check if the current data_addr has a non zero value.
        if !unsafe { self.load::<T>(data_addr).is_zero() } {
            return Some(data_addr.align_down(1 << self.log_bytes_in_region));
        }

        // Figure out the start and end data address.
        let start_addr = data_addr.saturating_sub(search_limit_bytes) + 1usize;
        let end_addr = data_addr;

        // Then figure out the start and end metadata address and bits.
        // The start bit may not be accurate, as we map any address in the region to the same bit.
        // We will filter the result at the end to make sure the found address is in the search range.
        let start_meta_addr = address_to_contiguous_meta_address(self, start_addr);
        let start_meta_shift = meta_byte_lshift(self, start_addr);
        let end_meta_addr = address_to_contiguous_meta_address(self, end_addr);
        let end_meta_shift = meta_byte_lshift(self, end_addr);

        let mut res = None;

        let mut visitor = |range: BitByteRange| {
            match range {
                BitByteRange::Bytes { start, end } => {
                    match helpers::find_last_non_zero_bit_in_metadata_bytes(start, end) {
                        helpers::FindMetaBitResult::Found { addr, bit } => {
                            let (addr, bit) = align_metadata_address(self, addr, bit);
                            res = Some(contiguous_meta_address_to_address(self, addr, bit));
                            // Return true to abort the search. We found the bit.
                            true
                        }
                        // If we see unmapped metadata, we don't need to search any more.
                        helpers::FindMetaBitResult::UnmappedMetadata => true,
                        // Return false to continue searching.
                        helpers::FindMetaBitResult::NotFound => false,
                    }
                }
                BitByteRange::BitsInByte {
                    addr,
                    bit_start,
                    bit_end,
                } => {
                    match helpers::find_last_non_zero_bit_in_metadata_bits(addr, bit_start, bit_end)
                    {
                        helpers::FindMetaBitResult::Found { addr, bit } => {
                            let (addr, bit) = align_metadata_address(self, addr, bit);
                            res = Some(contiguous_meta_address_to_address(self, addr, bit));
                            // Return true to abort the search. We found the bit.
                            true
                        }
                        // If we see unmapped metadata, we don't need to search any more.
                        helpers::FindMetaBitResult::UnmappedMetadata => true,
                        // Return false to continue searching.
                        helpers::FindMetaBitResult::NotFound => false,
                    }
                }
            }
        };

        ranges::break_bit_range(
            start_meta_addr,
            start_meta_shift,
            end_meta_addr,
            end_meta_shift,
            false,
            &mut visitor,
        );

        // We have to filter the result. We search between [start_addr, end_addr). But we actually
        // search with metadata bits. It is possible the metadata bit for start_addr is the same bit
        // as an address that is before start_addr. E.g. 0x2010f026360 and 0x2010f026361 are mapped
        // to the same bit, 0x2010f026361 is the start address and 0x2010f026360 is outside the search range.
        res.map(|addr| addr.align_down(1 << self.log_bytes_in_region))
            .filter(|addr| *addr >= start_addr && *addr < end_addr)
    }

    /// Search for data addresses that have non zero values in the side metadata.  This method is
    /// primarily used for heap traversal by scanning the VO bits.
    ///
    /// This function searches the side metadata for the data address range from `data_start_addr`
    /// (inclusive) to `data_end_addr` (exclusive).  The data address range must be fully mapped.
    ///
    /// For each data region that has non-zero side metadata, `visit_data` is called with the lowest
    /// address of that region.  Note that it may not be the original address used to set the
    /// metadata bits.
    pub fn scan_non_zero_values<T: MetadataValue>(
        &self,
        data_start_addr: Address,
        data_end_addr: Address,
        visit_data: &mut impl FnMut(Address),
    ) {
        if self.uses_contiguous_side_metadata() && self.log_num_of_bits == 0 {
            // Contiguous one-bit-per-region side metadata
            // TODO: VO bits is one-bit-per-word.  But if we want to scan other metadata (such as
            // the forwarding bits which has two bits per word), we will need to refactor the
            // algorithm of `scan_non_zero_values_fast`.
            self.scan_non_zero_values_fast(data_start_addr, data_end_addr, visit_data);
        } else {
            // TODO: VO bits are always contiguous.  But if we want to scan other metadata, such as
            // side mark bits, we need to refactor `bulk_update_metadata` to support `FnMut`, too,
            // and use it to apply `scan_non_zero_values_fast` on each contiguous side metadata
            // range.
            warn!(
                "We are trying to search for non zero bits in a discontiguous side metadata \
            or the metadata has more than one bit per region. \
                The performance is slow, as MMTk does not optimize for this case."
            );
            self.scan_non_zero_values_simple::<T>(data_start_addr, data_end_addr, visit_data);
        }
    }

    fn scan_non_zero_values_simple<T: MetadataValue>(
        &self,
        data_start_addr: Address,
        data_end_addr: Address,
        visit_data: &mut impl FnMut(Address),
    ) {
        let region_bytes = 1usize << self.log_bytes_in_region;

        let mut cursor = data_start_addr;
        while cursor < data_end_addr {
            debug_assert!(cursor.is_mapped());

            // If we find non-zero value, just call back.
            if !unsafe { self.load::<T>(cursor).is_zero() } {
                visit_data(cursor);
            }
            cursor += region_bytes;
        }
    }

    fn scan_non_zero_values_fast(
        &self,
        data_start_addr: Address,
        data_end_addr: Address,
        visit_data: &mut impl FnMut(Address),
    ) {
        debug_assert!(self.uses_contiguous_side_metadata());
        debug_assert_eq!(self.log_num_of_bits, 0);

        // Then figure out the start and end metadata address and bits.
        let start_meta_addr = address_to_contiguous_meta_address(self, data_start_addr);
        let start_meta_shift = meta_byte_lshift(self, data_start_addr);
        let end_meta_addr = address_to_contiguous_meta_address(self, data_end_addr);
        let end_meta_shift = meta_byte_lshift(self, data_end_addr);

        let mut visitor = |range| {
            match range {
                BitByteRange::Bytes { start, end } => {
                    helpers::scan_non_zero_bits_in_metadata_bytes(start, end, &mut |addr, bit| {
                        visit_data(helpers::contiguous_meta_address_to_address(self, addr, bit));
                    });
                }
                BitByteRange::BitsInByte {
                    addr,
                    bit_start,
                    bit_end,
                } => helpers::scan_non_zero_bits_in_metadata_bits(
                    addr,
                    bit_start,
                    bit_end,
                    &mut |addr, bit| {
                        visit_data(helpers::contiguous_meta_address_to_address(self, addr, bit));
                    },
                ),
            }
            false
        };

        ranges::break_bit_range(
            start_meta_addr,
            start_meta_shift,
            end_meta_addr,
            end_meta_shift,
            true,
            &mut visitor,
        );
    }
}

impl fmt::Debug for SideMetadataSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "SideMetadataSpec {} {{ \
            **is_global: {:?} \
            **offset: {} \
            **log_num_of_bits: 0x{:x} \
            **log_bytes_in_region: 0x{:x} \
            }}",
            self.name,
            self.is_global,
            unsafe {
                if self.is_absolute_offset() {
                    format!("0x{:x}", self.offset.addr)
                } else {
                    format!("0x{:x}", self.offset.rel_offset)
                }
            },
            self.log_num_of_bits,
            self.log_bytes_in_region
        ))
    }
}

/// A union of Address or relative offset (usize) used to store offset for a side metadata spec.
/// If a spec is contiguous side metadata, it uses address. Othrewise it uses usize.
// The fields are made private on purpose. They can only be accessed from SideMetadata which knows whether it is Address or usize.
#[derive(Clone, Copy)]
pub union SideMetadataOffset {
    addr: Address,
    rel_offset: usize,
}

impl SideMetadataOffset {
    /// Get an offset for a fixed address. This is usually used to set offset for the first spec (subsequent ones can be laid out with `layout_after`).
    pub const fn addr(addr: Address) -> Self {
        SideMetadataOffset { addr }
    }

    /// Get an offset for a relative offset (usize). This is usually used to set offset for the first spec (subsequent ones can be laid out with `layout_after`).
    pub const fn rel(rel_offset: usize) -> Self {
        SideMetadataOffset { rel_offset }
    }

    /// Get an offset after a spec. This is used to layout another spec immediately after this one.
    pub const fn layout_after(spec: &SideMetadataSpec) -> SideMetadataOffset {
        // Some metadata may be so small that its size is not a multiple of byte size.  One example
        // is `CHUNK_MARK`.  It is one byte per chunk.  However, on 32-bit architectures, we
        // allocate side metadata per chunk.  In that case, it will only occupy one byte.  If we
        // do not align the upper bound offset up, subsequent local metadata that need to be
        // accessed at, for example, word granularity will be misaligned.
        // TODO: Currently we align metadata to word size so that it is safe to access the metadata
        // one word at a time.  In the future, we may allow each metadata to specify its own
        // alignment requirement.
        let upper_bound_offset = spec.upper_bound_offset();
        if spec.is_absolute_offset() {
            let addr = unsafe { upper_bound_offset.addr };
            let aligned_addr = addr.align_up(BYTES_IN_WORD);
            SideMetadataOffset::addr(aligned_addr)
        } else {
            let rel_offset = unsafe { upper_bound_offset.rel_offset };
            let aligned_rel_offset = raw_align_up(rel_offset, BYTES_IN_WORD);
            SideMetadataOffset::rel(aligned_rel_offset)
        }
    }
}

// Address and usize has the same layout, so we use usize for implementing these traits.

impl PartialEq for SideMetadataOffset {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.rel_offset == other.rel_offset }
    }
}
impl Eq for SideMetadataOffset {}

impl std::hash::Hash for SideMetadataOffset {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        unsafe { self.rel_offset }.hash(state);
    }
}

/// This struct stores all the side metadata specs for a policy. Generally a policy needs to know its own
/// side metadata spec as well as the plan's specs.
pub(crate) struct SideMetadataContext {
    // For plans
    pub global: Vec<SideMetadataSpec>,
    // For policies
    pub local: Vec<SideMetadataSpec>,
}

impl SideMetadataContext {
    #[allow(clippy::vec_init_then_push)] // allow this, as we conditionally push based on features.
    pub fn new_global_specs(specs: &[SideMetadataSpec]) -> Vec<SideMetadataSpec> {
        let mut ret = vec![];

        #[cfg(feature = "vo_bit")]
        ret.push(VO_BIT_SIDE_METADATA_SPEC);

        if let Some(spec) = crate::mmtk::SFT_MAP.get_side_metadata() {
            if spec.is_global {
                ret.push(*spec);
            }
        }

        // Any plan that uses the chunk map needs to reserve the chunk map table.
        // As we use either the mark sweep or (non moving) immix as the non moving space,
        // and both policies use the chunk map, we just add the chunk map table globally.
        ret.push(crate::util::heap::chunk_map::ChunkMap::ALLOC_TABLE);

        ret.extend_from_slice(specs);
        ret
    }

    pub fn get_local_specs(&self) -> &[SideMetadataSpec] {
        &self.local
    }

    /// Return the pages reserved for side metadata based on the data pages we used.
    // We used to use PageAccouting to count pages used in side metadata. However,
    // that means we always count pages while we may reserve less than a page each time.
    // This could lead to overcount. I think the easier way is to not account
    // when we allocate for sidemetadata, but to calculate the side metadata usage based on
    // how many data pages we use when reporting.
    pub fn calculate_reserved_pages(&self, data_pages: usize) -> usize {
        let mut total = 0;
        for spec in self.global.iter() {
            // This rounds up.  No matter how small `data_pages` is, the side metadata size will be
            // at least one page.  This behavior is *intended*.  This over-estimated amount is used
            // for triggering GC and resizing the heap.
            total += data_to_meta_size_round_up(spec, data_pages);
        }
        for spec in self.local.iter() {
            total += data_to_meta_size_round_up(spec, data_pages);
        }
        total
    }

    // ** NOTE: **
    //  Regardless of the number of bits in a metadata unit, we always represent its content as a word.

    /// Tries to map the required metadata space and returns `true` is successful.
    /// This can be called at page granularity.
    pub fn try_map_metadata_space(
        &self,
        start: Address,
        size: usize,
        space_name: &str,
    ) -> Result<()> {
        debug!(
            "try_map_metadata_space({}, 0x{:x}, {}, {})",
            start,
            size,
            self.global.len(),
            self.local.len()
        );
        // Page aligned
        debug_assert!(start.is_aligned_to(BYTES_IN_PAGE));
        debug_assert!(size % BYTES_IN_PAGE == 0);
        self.map_metadata_internal(start, size, false, space_name)
    }

    /// Tries to map the required metadata address range, without reserving swap-space/physical memory for it.
    /// This will make sure the address range is exclusive to the caller. This should be called at chunk granularity.
    ///
    /// NOTE: Accessing addresses in this range will produce a segmentation fault if swap-space is not mapped using the `try_map_metadata_space` function.
    pub fn try_map_metadata_address_range(
        &self,
        start: Address,
        size: usize,
        name: &str,
    ) -> Result<()> {
        debug!(
            "try_map_metadata_address_range({}, 0x{:x}, {}, {})",
            start,
            size,
            self.global.len(),
            self.local.len()
        );
        // Chunk aligned
        debug_assert!(start.is_aligned_to(BYTES_IN_CHUNK));
        debug_assert!(size % BYTES_IN_CHUNK == 0);
        self.map_metadata_internal(start, size, true, name)
    }

    /// The internal function to mmap metadata
    ///
    /// # Arguments
    /// * `start` - The starting address of the source data.
    /// * `size` - The size of the source data (in bytes).
    /// * `no_reserve` - whether to invoke mmap with a noreserve flag (we use this flag to quarantine address range)
    /// * `space_name`: The name of the space, used for annotating the mmap.
    fn map_metadata_internal(
        &self,
        start: Address,
        size: usize,
        no_reserve: bool,
        space_name: &str,
    ) -> Result<()> {
        for spec in self.global.iter() {
            let anno = MmapAnnotation::SideMeta {
                space: space_name,
                meta: spec.name,
            };
            match try_mmap_contiguous_metadata_space(start, size, spec, no_reserve, &anno) {
                Ok(_) => {}
                Err(e) => return Result::Err(e),
            }
        }

        #[cfg(target_pointer_width = "32")]
        let mut lsize: usize = 0;

        for spec in self.local.iter() {
            // For local side metadata, we always have to reserve address space for all local
            // metadata required by all policies in MMTk to be able to calculate a constant offset
            // for each local metadata at compile-time (it's like assigning an ID to each policy).
            //
            // As the plan is chosen at run-time, we will never know which subset of policies will
            // be used during run-time. We can't afford this much address space in 32-bits.
            // So, we switch to the chunk-based approach for this specific case.
            //
            // The global metadata is different in that for each plan, we can calculate its constant
            // base addresses at compile-time. Using the chunk-based approach will need the same
            // address space size as the current not-chunked approach.
            #[cfg(target_pointer_width = "64")]
            {
                let anno = MmapAnnotation::SideMeta {
                    space: space_name,
                    meta: spec.name,
                };
                match try_mmap_contiguous_metadata_space(start, size, spec, no_reserve, &anno) {
                    Ok(_) => {}
                    Err(e) => return Result::Err(e),
                }
            }
            #[cfg(target_pointer_width = "32")]
            {
                lsize += metadata_bytes_per_chunk(spec.log_bytes_in_region, spec.log_num_of_bits);
            }
        }

        #[cfg(target_pointer_width = "32")]
        if lsize > 0 {
            let max = BYTES_IN_CHUNK >> super::constants::LOG_LOCAL_SIDE_METADATA_WORST_CASE_RATIO;
            debug_assert!(
                lsize <= max,
                "local side metadata per chunk (0x{:x}) must be less than (0x{:x})",
                lsize,
                max
            );
            // We are creating a mmap for all side metadata instead of one specific metadata.  We
            // just annotate it as "all" here.
            let anno = MmapAnnotation::SideMeta {
                space: space_name,
                meta: "all",
            };
            match try_map_per_chunk_metadata_space(start, size, lsize, no_reserve, &anno) {
                Ok(_) => {}
                Err(e) => return Result::Err(e),
            }
        }

        Ok(())
    }

    /// Unmap the corresponding metadata space or panic.
    ///
    /// Note-1: This function is only used for test and debug right now.
    ///
    /// Note-2: This function uses munmap() which works at page granularity.
    ///     If the corresponding metadata space's size is not a multiple of page size,
    ///     the actual unmapped space will be bigger than what you specify.
    #[cfg(test)]
    pub fn ensure_unmap_metadata_space(&self, start: Address, size: usize) {
        trace!("ensure_unmap_metadata_space({}, 0x{:x})", start, size);
        debug_assert!(start.is_aligned_to(BYTES_IN_PAGE));
        debug_assert!(size % BYTES_IN_PAGE == 0);

        for spec in self.global.iter() {
            ensure_munmap_contiguous_metadata_space(start, size, spec);
        }

        for spec in self.local.iter() {
            #[cfg(target_pointer_width = "64")]
            {
                ensure_munmap_contiguous_metadata_space(start, size, spec);
            }
            #[cfg(target_pointer_width = "32")]
            {
                ensure_munmap_chunked_metadata_space(start, size, spec);
            }
        }
    }
}

/// A byte array in side-metadata
pub struct MetadataByteArrayRef<const ENTRIES: usize> {
    #[cfg(feature = "extreme_assertions")]
    heap_range_start: Address,
    #[cfg(feature = "extreme_assertions")]
    spec: SideMetadataSpec,
    data: &'static [u8; ENTRIES],
}

impl<const ENTRIES: usize> MetadataByteArrayRef<ENTRIES> {
    /// Get a piece of metadata address range as a byte array.
    ///
    /// # Arguments
    ///
    /// * `metadata_spec` - The specification of the target side metadata.
    /// * `start` - The starting address of the heap range.
    /// * `bytes` - The size of the heap range.
    ///
    pub fn new(metadata_spec: &SideMetadataSpec, start: Address, bytes: usize) -> Self {
        debug_assert_eq!(
            metadata_spec.log_num_of_bits, LOG_BITS_IN_BYTE as usize,
            "Each heap entry should map to a byte in side-metadata"
        );
        debug_assert_eq!(
            bytes >> metadata_spec.log_bytes_in_region,
            ENTRIES,
            "Heap range size and MetadataByteArray size does not match"
        );
        Self {
            #[cfg(feature = "extreme_assertions")]
            heap_range_start: start,
            #[cfg(feature = "extreme_assertions")]
            spec: *metadata_spec,
            // # Safety
            // The metadata memory is assumed to be mapped when accessing.
            data: unsafe { &*address_to_meta_address(metadata_spec, start).to_ptr() },
        }
    }

    /// Get the length of the array.
    #[allow(clippy::len_without_is_empty)]
    pub const fn len(&self) -> usize {
        ENTRIES
    }

    /// Get a byte from the metadata byte array at the given index.
    #[allow(clippy::let_and_return)]
    pub fn get(&self, index: usize) -> u8 {
        #[cfg(feature = "extreme_assertions")]
        let _lock = sanity::SANITY_LOCK.lock().unwrap();
        let value = self.data[index];
        #[cfg(feature = "extreme_assertions")]
        {
            let data_addr = self.heap_range_start + (index << self.spec.log_bytes_in_region);
            sanity::verify_load::<u8>(&self.spec, data_addr, value);
        }
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mmap_anno_test;
    use crate::util::metadata::side_metadata::SideMetadataContext;

    // offset is not used in these tests.
    pub const ZERO_OFFSET: SideMetadataOffset = SideMetadataOffset { rel_offset: 0 };

    #[test]
    fn calculate_reserved_pages_one_spec() {
        // 1 bit per 8 bytes - 1:64
        let spec = SideMetadataSpec {
            name: "test_spec",
            is_global: true,
            offset: ZERO_OFFSET,
            log_num_of_bits: 0,
            log_bytes_in_region: 3,
        };
        let side_metadata = SideMetadataContext {
            global: vec![spec],
            local: vec![],
        };
        assert_eq!(side_metadata.calculate_reserved_pages(0), 0);
        assert_eq!(side_metadata.calculate_reserved_pages(63), 1);
        assert_eq!(side_metadata.calculate_reserved_pages(64), 1);
        assert_eq!(side_metadata.calculate_reserved_pages(65), 2);
        assert_eq!(side_metadata.calculate_reserved_pages(1024), 16);
    }

    #[test]
    fn calculate_reserved_pages_multi_specs() {
        // 1 bit per 8 bytes - 1:64
        let gspec = SideMetadataSpec {
            name: "gspec",
            is_global: true,
            offset: ZERO_OFFSET,
            log_num_of_bits: 0,
            log_bytes_in_region: 3,
        };
        // 2 bits per page - 2 / (4k * 8) = 1:16k
        let lspec = SideMetadataSpec {
            name: "lspec",
            is_global: false,
            offset: ZERO_OFFSET,
            log_num_of_bits: 1,
            log_bytes_in_region: 12,
        };
        let side_metadata = SideMetadataContext {
            global: vec![gspec],
            local: vec![lspec],
        };
        assert_eq!(side_metadata.calculate_reserved_pages(1024), 16 + 1);
    }

    use crate::util::heap::layout::vm_layout;
    use crate::util::test_util::{serial_test, with_cleanup};
    use memory::MmapStrategy;
    use paste::paste;

    const TEST_LOG_BYTES_IN_REGION: usize = 12;

    fn test_side_metadata(
        log_bits: usize,
        f: impl Fn(&SideMetadataSpec, Address, Address) + std::panic::RefUnwindSafe,
    ) {
        serial_test(|| {
            let spec = SideMetadataSpec {
                name: "Test Spec $tname",
                is_global: true,
                offset: SideMetadataOffset::addr(GLOBAL_SIDE_METADATA_BASE_ADDRESS),
                log_num_of_bits: log_bits,
                log_bytes_in_region: TEST_LOG_BYTES_IN_REGION, // page size
            };
            let context = SideMetadataContext {
                global: vec![spec],
                local: vec![],
            };
            let mut sanity = SideMetadataSanity::new();
            sanity.verify_metadata_context("TestPolicy", &context);

            let data_addr = vm_layout::vm_layout().heap_start;
            // Make sure the address is mapped.
            crate::MMAPPER
                .ensure_mapped(data_addr, 1, MmapStrategy::TEST, mmap_anno_test!())
                .unwrap();
            let meta_addr = address_to_meta_address(&spec, data_addr);
            with_cleanup(
                || {
                    let mmap_result =
                        context.try_map_metadata_space(data_addr, BYTES_IN_PAGE, "test_space");
                    assert!(mmap_result.is_ok());

                    f(&spec, data_addr, meta_addr);
                },
                || {
                    // Clear the metadata -- use u64 (max length we support)
                    assert!(log_bits <= 6);
                    let meta_ptr: *mut u64 = meta_addr.to_mut_ptr();
                    unsafe { *meta_ptr = 0 };

                    sanity::reset();
                },
            )
        })
    }

    fn max_value(log_bits: usize) -> u64 {
        (0..(1 << log_bits)).fold(0, |accum, x| accum + (1 << x))
    }
    #[test]
    fn test_max_value() {
        assert_eq!(max_value(0), 1);
        assert_eq!(max_value(1), 0b11);
        assert_eq!(max_value(2), 0b1111);
        assert_eq!(max_value(3), 255);
        assert_eq!(max_value(4), 65535);
    }

    macro_rules! test_side_metadata_access {
        ($tname: ident, $type: ty, $log_bits: expr) => {
            paste!{
                #[test]
                fn [<$tname _load>]() {
                    test_side_metadata($log_bits, |spec, data_addr, meta_addr| {
                        let meta_ptr: *mut $type = meta_addr.to_mut_ptr();

                        // Initial value should be 0
                        assert_eq!(unsafe { spec.load::<$type>(data_addr) }, 0);
                        assert_eq!(spec.load_atomic::<$type>(data_addr, Ordering::SeqCst), 0);

                        // Set to max
                        let max_value: $type = max_value($log_bits) as _;
                        unsafe { spec.store::<$type>(data_addr, max_value); }
                        assert_eq!(unsafe { spec.load::<$type>(data_addr) }, max_value);
                        assert_eq!(spec.load_atomic::<$type>(data_addr, Ordering::SeqCst), max_value);
                        assert_eq!(unsafe { *meta_ptr }, max_value);
                    });
                }

                #[test]
                fn [<$tname _store>]() {
                    test_side_metadata($log_bits, |spec, data_addr, meta_addr| {
                        let meta_ptr: *mut $type = meta_addr.to_mut_ptr();
                        let max_value: $type = max_value($log_bits) as _;

                        // Set the metadata byte(s) to all 1s
                        unsafe { *meta_ptr = <$type>::MAX; }
                        // Store 0 to the side metadata
                        unsafe { spec.store::<$type>(data_addr, 0); }
                        assert_eq!(unsafe { spec.load::<$type>(data_addr) }, 0);
                        // Only the affected bits are set to 0
                        assert_eq!(unsafe { *meta_ptr }, <$type>::MAX & (!max_value));
                    });
                }

                #[test]
                fn [<$tname _atomic_store>]() {
                    test_side_metadata($log_bits, |spec, data_addr, meta_addr| {
                        let meta_ptr: *mut $type = meta_addr.to_mut_ptr();
                        let max_value: $type = max_value($log_bits) as _;

                        // Set the metadata byte(s) to all 1s
                        unsafe { *meta_ptr = <$type>::MAX; }
                        // Store 0 to the side metadata
                        spec.store_atomic::<$type>(data_addr, 0, Ordering::SeqCst);
                        assert_eq!(unsafe { spec.load::<$type>(data_addr) }, 0);
                        // Only the affected bits are set to 0
                        assert_eq!(unsafe { *meta_ptr }, <$type>::MAX & (!max_value));
                    });
                }

                #[test]
                fn [<$tname _compare_exchange_success>]() {
                    test_side_metadata($log_bits, |spec, data_addr, meta_addr| {
                        let meta_ptr: *mut $type = meta_addr.to_mut_ptr();
                        let max_value: $type = max_value($log_bits) as _;
                        // Set the metadata byte(s) to all 1s
                        unsafe { *meta_ptr = <$type>::MAX; }
                        // Store 1 to the side metadata
                        spec.store_atomic::<$type>(data_addr, 1, Ordering::SeqCst);

                        let old_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);
                        assert_eq!(old_val, 1);

                        let new_val = 0;
                        let res = spec.compare_exchange_atomic::<$type>(data_addr, old_val, new_val, Ordering::SeqCst, Ordering::SeqCst);
                        assert!(res.is_ok());
                        assert_eq!(res.unwrap(), old_val, "old vals do not match");

                        let after_update = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);
                        assert_eq!(after_update, new_val);
                        // Only the affected bits are set to 0
                        assert_eq!(unsafe { *meta_ptr }, <$type>::MAX & (!max_value));
                    });
                }

                #[test]
                fn [<$tname _compare_exchange_fail>]() {
                    test_side_metadata($log_bits, |spec, data_addr, meta_addr| {
                        let meta_ptr: *mut $type = meta_addr.to_mut_ptr();
                        // Set the metadata byte(s) to all 1s
                        unsafe { *meta_ptr = <$type>::MAX; }
                        // Store 1 to the side metadata
                        spec.store_atomic::<$type>(data_addr, 1, Ordering::SeqCst);

                        let old_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);
                        assert_eq!(old_val, 1);

                        // make old_val outdated
                        spec.store_atomic::<$type>(data_addr, 0, Ordering::SeqCst);
                        let bits_before_cas = unsafe { *meta_ptr };

                        let new_val = 0;
                        let res = spec.compare_exchange_atomic::<$type>(data_addr, old_val, new_val, Ordering::SeqCst, Ordering::SeqCst);
                        assert!(res.is_err());
                        assert_eq!(res.err().unwrap(), 0);
                        let bits_after_cas = unsafe { *meta_ptr };
                        assert_eq!(bits_before_cas, bits_after_cas);
                    });
                }

                #[test]
                fn [<$tname _fetch_add_1>]() {
                    test_side_metadata($log_bits, |spec, data_addr, meta_addr| {
                        let meta_ptr: *mut $type = meta_addr.to_mut_ptr();
                        // Set the metadata byte(s) to all 1s
                        unsafe { *meta_ptr = <$type>::MAX; }
                        // Store 0 to the side metadata
                        spec.store_atomic::<$type>(data_addr, 0, Ordering::SeqCst);

                        let old_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);

                        let old_val_from_fetch = spec.fetch_add_atomic::<$type>(data_addr, 1, Ordering::SeqCst);
                        assert_eq!(old_val_from_fetch, old_val);

                        let new_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);
                        assert_eq!(new_val, 1);
                    });
                }

                #[test]
                fn [<$tname _fetch_add_max>]() {
                    test_side_metadata($log_bits, |spec, data_addr, meta_addr| {
                        let meta_ptr: *mut $type = meta_addr.to_mut_ptr();
                        let max_value: $type = max_value($log_bits) as _;
                        // Set the metadata byte(s) to all 1s
                        unsafe { *meta_ptr = <$type>::MAX; }
                        // Store 0 to the side metadata
                        spec.store_atomic::<$type>(data_addr, 0, Ordering::SeqCst);

                        let old_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);

                        let old_val_from_fetch = spec.fetch_add_atomic::<$type>(data_addr, max_value, Ordering::SeqCst);
                        assert_eq!(old_val_from_fetch, old_val);

                        let new_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);
                        assert_eq!(new_val, max_value);
                    });
                }

                #[test]
                fn [<$tname _fetch_add_overflow>]() {
                    test_side_metadata($log_bits, |spec, data_addr, meta_addr| {
                        let meta_ptr: *mut $type = meta_addr.to_mut_ptr();
                        let max_value: $type = max_value($log_bits) as _;
                        // Set the metadata byte(s) to all 1s
                        unsafe { *meta_ptr = <$type>::MAX; }
                        // Store max to the side metadata
                        spec.store_atomic::<$type>(data_addr, max_value, Ordering::SeqCst);

                        let old_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);

                        // add 1 to max value will cause overflow and wrap around to 0
                        let old_val_from_fetch = spec.fetch_add_atomic::<$type>(data_addr, 1, Ordering::SeqCst);
                        assert_eq!(old_val_from_fetch, old_val);

                        let new_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);
                        assert_eq!(new_val, 0);
                    });
                }

                #[test]
                fn [<$tname _fetch_sub_1>]() {
                    test_side_metadata($log_bits, |spec, data_addr, meta_addr| {
                        let meta_ptr: *mut $type = meta_addr.to_mut_ptr();
                        // Set the metadata byte(s) to all 1s
                        unsafe { *meta_ptr = <$type>::MAX; }
                        // Store 1 to the side metadata
                        spec.store_atomic::<$type>(data_addr, 1, Ordering::SeqCst);

                        let old_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);

                        let old_val_from_fetch = spec.fetch_sub_atomic::<$type>(data_addr, 1, Ordering::SeqCst);
                        assert_eq!(old_val_from_fetch, old_val);

                        let new_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);
                        assert_eq!(new_val, 0);
                    });
                }

                #[test]
                fn [<$tname _fetch_sub_max>]() {
                    test_side_metadata($log_bits, |spec, data_addr, meta_addr| {
                        let meta_ptr: *mut $type = meta_addr.to_mut_ptr();
                        let max_value: $type = max_value($log_bits) as _;
                        // Set the metadata byte(s) to all 1s
                        unsafe { *meta_ptr = <$type>::MAX; }
                        // Store max to the side metadata
                        spec.store_atomic::<$type>(data_addr, max_value, Ordering::SeqCst);

                        let old_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);

                        let old_val_from_fetch = spec.fetch_sub_atomic::<$type>(data_addr, max_value, Ordering::SeqCst);
                        assert_eq!(old_val_from_fetch, old_val);

                        let new_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);
                        assert_eq!(new_val, 0);
                    });
                }

                #[test]
                fn [<$tname _fetch_sub_overflow>]() {
                    test_side_metadata($log_bits, |spec, data_addr, meta_addr| {
                        let meta_ptr: *mut $type = meta_addr.to_mut_ptr();
                        let max_value: $type = max_value($log_bits) as _;
                        // Set the metadata byte(s) to all 1s
                        unsafe { *meta_ptr = <$type>::MAX; }
                        // Store 0 to the side metadata
                        spec.store_atomic::<$type>(data_addr, 0, Ordering::SeqCst);

                        let old_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);

                        // sub 1 from 0 will cause overflow, and wrap around to max
                        let old_val_from_fetch = spec.fetch_sub_atomic::<$type>(data_addr, 1, Ordering::SeqCst);
                        assert_eq!(old_val_from_fetch, old_val);

                        let new_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);
                        assert_eq!(new_val, max_value);
                    });
                }

                #[test]
                fn [<$tname _fetch_and>]() {
                    test_side_metadata($log_bits, |spec, data_addr, meta_addr| {
                        let meta_ptr: *mut $type = meta_addr.to_mut_ptr();
                        let max_value: $type = max_value($log_bits) as _;
                        // Set the metadata byte(s) to all 1s
                        unsafe { *meta_ptr = <$type>::MAX; }
                        // Store all 1s to the side metadata
                        spec.store_atomic::<$type>(data_addr, max_value, Ordering::SeqCst);

                        // max and max should be max
                        let old_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);
                        let old_val_from_fetch = spec.fetch_and_atomic::<$type>(data_addr, max_value, Ordering::SeqCst);
                        assert_eq!(old_val_from_fetch, old_val, "old values do not match");
                        assert_eq!(spec.load_atomic::<$type>(data_addr, Ordering::SeqCst), max_value, "load values do not match");
                        assert_eq!(unsafe { *meta_ptr }, <$type>::MAX, "raw values do not match");

                        // max and last_bit_zero should last_bit_zero
                        let last_bit_zero = max_value - 1;
                        let old_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);
                        let old_val_from_fetch = spec.fetch_and_atomic::<$type>(data_addr, last_bit_zero, Ordering::SeqCst);
                        assert_eq!(old_val_from_fetch, old_val);
                        assert_eq!(spec.load_atomic::<$type>(data_addr, Ordering::SeqCst), last_bit_zero);
                        assert_eq!(unsafe { *meta_ptr }, <$type>::MAX - 1);
                    });
                }

                #[test]
                fn [<$tname _fetch_or>]() {
                    test_side_metadata($log_bits, |spec, data_addr, meta_addr| {
                        let meta_ptr: *mut $type = meta_addr.to_mut_ptr();
                        let max_value: $type = max_value($log_bits) as _;
                        // Set the metadata byte(s) to all 0s
                        unsafe { *meta_ptr = 0; }
                        // Store 0 to the side metadata
                        spec.store_atomic::<$type>(data_addr, 0, Ordering::SeqCst);

                        // 0 or 0 should be 0
                        let old_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);
                        let old_val_from_fetch = spec.fetch_or_atomic::<$type>(data_addr, 0, Ordering::SeqCst);
                        assert_eq!(old_val_from_fetch, old_val);
                        assert_eq!(spec.load_atomic::<$type>(data_addr, Ordering::SeqCst), 0);
                        assert_eq!(unsafe { *meta_ptr }, 0);

                        // 0 and max should max
                        let old_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);
                        let old_val_from_fetch = spec.fetch_or_atomic::<$type>(data_addr, max_value, Ordering::SeqCst);
                        assert_eq!(old_val_from_fetch, old_val);
                        assert_eq!(spec.load_atomic::<$type>(data_addr, Ordering::SeqCst), max_value);
                        assert_eq!(unsafe { *meta_ptr }, max_value);
                    });
                }

                #[test]
                fn [<$tname _fetch_update_success>]() {
                    test_side_metadata($log_bits, |spec, data_addr, meta_addr| {
                        let meta_ptr: *mut $type = meta_addr.to_mut_ptr();
                        let max_value: $type = max_value($log_bits) as _;
                        // Set the metadata byte(s) to all 1s
                        unsafe { *meta_ptr = <$type>::MAX; }
                        // Store all 1s to the side metadata
                        spec.store_atomic::<$type>(data_addr, max_value, Ordering::SeqCst);

                        // update from max to zero
                        let old_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);
                        let fetch_res = spec.fetch_update_atomic::<$type, _>(data_addr, Ordering::SeqCst, Ordering::SeqCst, |_x: $type| Some(0));
                        assert!(fetch_res.is_ok());
                        assert_eq!(fetch_res.unwrap(), old_val);
                        assert_eq!(spec.load_atomic::<$type>(data_addr, Ordering::SeqCst), 0);
                        // Only the affected bits are set to 0
                        assert_eq!(unsafe { *meta_ptr }, <$type>::MAX & (!max_value));
                    });
                }

                #[test]
                fn [<$tname _fetch_update_fail>]() {
                    test_side_metadata($log_bits, |spec, data_addr, meta_addr| {
                        let meta_ptr: *mut $type = meta_addr.to_mut_ptr();
                        let max_value: $type = max_value($log_bits) as _;
                        // Set the metadata byte(s) to all 1s
                        unsafe { *meta_ptr = <$type>::MAX; }
                        // Store all 1s to the side metadata
                        spec.store_atomic::<$type>(data_addr, max_value, Ordering::SeqCst);

                        // update from max to zero
                        let old_val = spec.load_atomic::<$type>(data_addr, Ordering::SeqCst);
                        let fetch_res = spec.fetch_update_atomic::<$type, _>(data_addr, Ordering::SeqCst, Ordering::SeqCst, |_x: $type| None);
                        assert!(fetch_res.is_err());
                        assert_eq!(fetch_res.err().unwrap(), old_val);
                        assert_eq!(spec.load_atomic::<$type>(data_addr, Ordering::SeqCst), max_value);
                        // Only the affected bits are set to 0
                        assert_eq!(unsafe { *meta_ptr }, <$type>::MAX);
                    });
                }

                #[test]
                fn [<$tname _find_prev_non_zero_value_easy>]() {
                    test_side_metadata($log_bits, |spec, data_addr, _meta_addr| {
                        let max_value: $type = max_value($log_bits) as _;
                        // Store non zero value at data_addr
                        spec.store_atomic::<$type>(data_addr, max_value, Ordering::SeqCst);

                        // Find the value starting from data_addr, at max 8 bytes.
                        // We should find data_addr
                        let res_addr = unsafe { spec.find_prev_non_zero_value::<$type>(data_addr, 8) };
                        assert!(res_addr.is_some());
                        assert_eq!(res_addr.unwrap(), data_addr);
                    });
                }

                #[test]
                fn [<$tname _find_prev_non_zero_value_arbitrary_bytes>]() {
                    test_side_metadata($log_bits, |spec, data_addr, _meta_addr| {
                        let max_value: $type = max_value($log_bits) as _;
                        // Store non zero value at data_addr
                        spec.store_atomic::<$type>(data_addr, max_value, Ordering::SeqCst);

                        // Start from data_addr, we offset arbitrary length, and search back to find data_addr
                        let test_region = (1 << TEST_LOG_BYTES_IN_REGION);
                        for len in 1..(test_region*4) {
                            let start_addr = data_addr + len;
                            // Use len+1, as len is non inclusive.
                            let res_addr = unsafe { spec.find_prev_non_zero_value::<$type>(start_addr, len + 1) };
                            assert!(res_addr.is_some());
                            assert_eq!(res_addr.unwrap(), data_addr);
                        }
                    });
                }

                #[test]
                fn [<$tname _find_prev_non_zero_value_arbitrary_start>]() {
                    test_side_metadata($log_bits, |spec, data_addr, _meta_addr| {
                        let max_value: $type = max_value($log_bits) as _;

                        // data_addr has a non-aligned offset
                        for offset in 0..7usize {
                            // Apply offset and test with the new data addr
                            let test_data_addr = data_addr + offset;
                            spec.store_atomic::<$type>(test_data_addr, max_value, Ordering::SeqCst);

                            // The return result should be aligned
                            let res_addr = unsafe { spec.find_prev_non_zero_value::<$type>(test_data_addr, 4096) };
                            assert!(res_addr.is_some());
                            assert_eq!(res_addr.unwrap(), data_addr);

                            // Clear whatever is set
                            spec.store_atomic::<$type>(test_data_addr, 0, Ordering::SeqCst);
                        }
                    });
                }

                #[test]
                fn [<$tname _find_prev_non_zero_value_no_find>]() {
                    test_side_metadata($log_bits, |spec, data_addr, _meta_addr| {
                        // Store zero value at data_addr -- so we won't find anything
                        spec.store_atomic::<$type>(data_addr, 0, Ordering::SeqCst);

                        // Start from data_addr, we offset arbitrary length, and search back
                        let test_region = (1 << TEST_LOG_BYTES_IN_REGION);
                        for len in 1..(test_region*4) {
                            let start_addr = data_addr + len;
                            // Use len+1, as len is non inclusive.
                            let res_addr = unsafe { spec.find_prev_non_zero_value::<$type>(start_addr, len + 1) };
                            assert!(res_addr.is_none());
                        }
                    });
                }
            }
        }
    }

    test_side_metadata_access!(test_u1, u8, 0);
    test_side_metadata_access!(test_u2, u8, 1);
    test_side_metadata_access!(test_u4, u8, 2);
    test_side_metadata_access!(test_u8, u8, 3);
    test_side_metadata_access!(test_u16, u16, 4);
    test_side_metadata_access!(test_u32, u32, 5);
    test_side_metadata_access!(test_u64, u64, 6);
    test_side_metadata_access!(
        test_usize,
        usize,
        if cfg!(target_pointer_width = "64") {
            6
        } else if cfg!(target_pointer_width = "32") {
            5
        } else {
            unreachable!()
        }
    );

    #[test]
    fn test_bulk_update_meta_bits() {
        let raw_mem =
            unsafe { std::alloc::alloc_zeroed(std::alloc::Layout::from_size_align(8, 8).unwrap()) };
        let addr = Address::from_mut_ptr(raw_mem);

        SideMetadataSpec::set_meta_bits(addr, 0, addr, 4);
        assert_eq!(unsafe { addr.load::<u64>() }, 0b1111);

        SideMetadataSpec::zero_meta_bits(addr, 1, addr, 3);
        assert_eq!(unsafe { addr.load::<u64>() }, 0b1001);

        SideMetadataSpec::set_meta_bits(addr, 2, addr, 6);
        assert_eq!(unsafe { addr.load::<u64>() }, 0b0011_1101);

        SideMetadataSpec::zero_meta_bits(addr, 0, addr + 1usize, 0);
        assert_eq!(unsafe { addr.load::<u64>() }, 0b0);

        SideMetadataSpec::set_meta_bits(addr, 2, addr + 1usize, 2);
        assert_eq!(unsafe { addr.load::<u64>() }, 0b11_1111_1100);

        SideMetadataSpec::set_meta_bits(addr, 0, addr + 1usize, 2);
        assert_eq!(unsafe { addr.load::<u64>() }, 0b11_1111_1111);
    }
}
