use crate::util::copy::*;
use crate::util::metadata::MetadataSpec;
use crate::util::{constants, Address, ObjectReference};
use crate::vm::ObjectModel;
use crate::vm::VMBinding;
use std::sync::atomic::Ordering;

const FORWARDING_NOT_TRIGGERED_YET: u8 = 0b00;
const BEING_FORWARDED: u8 = 0b10;
const FORWARDED: u8 = 0b11;
const FORWARDING_MASK: u8 = 0b11;
#[allow(unused)]
const FORWARDING_BITS: usize = 2;

const CLAIMED_FORWARDING_POINTER: u32 = 0x8;
const CLAIMED_FORWARDING_POINTER_AS_ADDRESS: Address =
    unsafe { Address::from_usize(CLAIMED_FORWARDING_POINTER as usize) };
const FORWARDING_BIT_SIZE: u32 = 1;
const FORWARDING_BIT_SHIFT: u32 = 29;
const FORWARDING_BIT_MASK: u32 = (1 << FORWARDING_BIT_SIZE) - 1;
const FORWARDING_BIT_MASK_SHIFTED: u32 = FORWARDING_BIT_MASK << FORWARDING_BIT_SHIFT;
const FORWARDING_BIT_MASK_SHIFTED_TOGGLED: u32 = !FORWARDING_BIT_MASK_SHIFTED;
const FORWARDING_ADDRESS_SHIFT: u32 = 0x3;

// copy address mask
// #[cfg(target_pointer_width = "64")]
// const FORWARDING_POINTER_MASK: usize = 0x00ff_ffff_ffff_fff8;
// #[cfg(target_pointer_width = "32")]
const FORWARDING_POINTER_MASK: u32 = 0x1fff_ffff;

/// Attempt to become the worker thread who will forward the object.
/// The successful worker will set the object forwarding bits to BEING_FORWARDED, preventing other workers from forwarding the same object.
pub fn attempt_to_forward<VM: VMBinding>(
    object: ObjectReference,
    mark_word: u32,
) -> Option<ObjectReference> {
    let mut old_mark = mark_word;
    while !is_marked(old_mark) {
        match VM::VMObjectModel::LOCAL_FORWARDING_POINTER_SPEC.compare_exchange_metadata::<VM, u32>(
            object,
            old_mark,
            (0b1 << FORWARDING_BIT_SHIFT)
                | (CLAIMED_FORWARDING_POINTER >> FORWARDING_ADDRESS_SHIFT),
            None,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            Ok(_) => {
                return None;
            }
            Err(new_mark) => {
                // Failed the race, but we need to check if it has been marked
                old_mark = new_mark;
            }
        }
    }

    Some(read_forwarding_pointer::<VM>(object))
}

/// Spin-wait for the object's forwarding to become complete and then read the forwarding pointer to the new object.
///
/// # Arguments:
///
/// * `object`: the forwarded/being_forwarded object.
/// * `forwarding_bits`: the last state of the forwarding bits before calling this function.
///
/// Returns a reference to the new object.
///
pub fn spin_and_get_forwarded_object<VM: VMBinding>(object: ObjectReference) -> ObjectReference {
    let mut mark_word = unsafe { get_mark_word_nonatomic::<VM>(object) };
    while state_is_being_forwarded(mark_word) {
        // XXX(kunals): Need to use relaxed ordering here because if we use non-atomic access,
        // the compiler might optimize it out
        mark_word = get_mark_word_relaxed::<VM>(object);
    }

    if state_is_forwarded(mark_word) {
        unsafe {
            // We use "unchecked" conversion because we guarantee the forwarding pointer we stored
            // previously is from a valid `ObjectReference` which is never zero.
            ObjectReference::from_raw_address_unchecked(unsafe {
                Address::from_usize(
                    ((mark_word & FORWARDING_POINTER_MASK) << FORWARDING_ADDRESS_SHIFT) as usize,
                )
            })
        }
    } else {
        // For some policies (such as Immix), we can have interleaving such that one thread clears
        // the forwarding word while another thread was stuck spinning in the above loop.
        // See: https://github.com/mmtk/mmtk-core/issues/579
        debug_assert!(
            !state_is_forwarded_or_being_forwarded(mark_word),
            "Invalid/Corrupted mark word {:x} for object {}",
            mark_word,
            object,
        );
        object
    }
}

pub fn forward_object<VM: VMBinding>(
    object: ObjectReference,
    old_mark_word: u32,
    semantics: CopySemantics,
    copy_context: &mut GCWorkerCopyContext<VM>,
) -> ObjectReference {
    let new_object = VM::VMObjectModel::copy(object, semantics, copy_context);
    write_forwarding_pointer::<VM>(object, new_object);
    set_mark_word::<VM>(new_object, old_mark_word);
    new_object
}

fn is_marked(mark_word: u32) -> bool {
    (mark_word >> FORWARDING_BIT_SHIFT) & FORWARDING_BIT_MASK == 0b1
}

/// Return the forwarding bits for a given `ObjectReference`. Note that this function is unsafe as
/// it uses nonatomic accesses.
pub unsafe fn get_mark_word_nonatomic<VM: VMBinding>(object: ObjectReference) -> u32 {
    VM::VMObjectModel::LOCAL_FORWARDING_POINTER_SPEC.load::<VM, u32>(object, None)
}

fn get_mark_word_relaxed<VM: VMBinding>(object: ObjectReference) -> u32 {
    VM::VMObjectModel::LOCAL_FORWARDING_POINTER_SPEC.load_atomic::<VM, u32>(
        object,
        None,
        Ordering::Relaxed,
    )
}

/// Return the forwarding bits for a given `ObjectReference`.
pub fn get_mark_word<VM: VMBinding>(object: ObjectReference) -> u32 {
    VM::VMObjectModel::LOCAL_FORWARDING_POINTER_SPEC.load_atomic::<VM, u32>(
        object,
        None,
        Ordering::SeqCst,
    )
}

pub fn set_mark_word<VM: VMBinding>(object: ObjectReference, mark_word: u32) {
    VM::VMObjectModel::LOCAL_FORWARDING_POINTER_SPEC.store_atomic::<VM, u32>(
        object,
        mark_word,
        None,
        Ordering::SeqCst,
    )
}

/// Return the forwarding bits for a given `ObjectReference`.
pub fn get_forwarding_status<VM: VMBinding>(object: ObjectReference) -> u8 {
    VM::VMObjectModel::LOCAL_FORWARDING_BITS_SPEC.load_atomic::<VM, u8>(
        object,
        None,
        Ordering::SeqCst,
    )
}

pub fn is_forwarded<VM: VMBinding>(object: ObjectReference) -> bool {
    let mark_word = get_mark_word::<VM>(object);
    is_marked(mark_word) && ((mark_word << FORWARDING_ADDRESS_SHIFT) != CLAIMED_FORWARDING_POINTER)
}

fn is_being_forwarded<VM: VMBinding>(object: ObjectReference) -> bool {
    let mark_word = get_mark_word::<VM>(object);
    is_marked(mark_word) && ((mark_word << FORWARDING_ADDRESS_SHIFT) == CLAIMED_FORWARDING_POINTER)
}

pub fn is_forwarded_or_being_forwarded<VM: VMBinding>(object: ObjectReference) -> bool {
    is_marked(get_mark_word::<VM>(object))
}

fn state_is_forwarded(mark_word: u32) -> bool {
    is_marked(mark_word) && ((mark_word << FORWARDING_ADDRESS_SHIFT) != CLAIMED_FORWARDING_POINTER)
}

fn state_is_being_forwarded(mark_word: u32) -> bool {
    is_marked(mark_word) && ((mark_word << FORWARDING_ADDRESS_SHIFT) == CLAIMED_FORWARDING_POINTER)
}

fn state_is_forwarded_or_being_forwarded(mark_word: u32) -> bool {
    is_marked(mark_word)
}

/// Zero the forwarding bits of an object.
/// This function is used on new objects.
pub fn clear_forwarding_bits<VM: VMBinding>(object: ObjectReference) {
    VM::VMObjectModel::LOCAL_FORWARDING_BITS_SPEC.store_atomic::<VM, u8>(
        object,
        0,
        None,
        Ordering::SeqCst,
    )
}

/// Read the forwarding pointer of an object.
/// This function is called on forwarded/being_forwarded objects.
pub fn read_forwarding_pointer<VM: VMBinding>(object: ObjectReference) -> ObjectReference {
    debug_assert!(
        is_forwarded_or_being_forwarded::<VM>(object),
        "read_forwarding_pointer called for object {:?} that has not started forwarding!",
        object,
    );

    // We write the forwarding poiner. We know it is an object reference.
    unsafe {
        // We use "unchecked" convertion becasue we guarantee the forwarding pointer we stored
        // previously is from a valid `ObjectReference` which is never zero.
        ObjectReference::from_raw_address_unchecked(crate::util::Address::from_usize(
            (VM::VMObjectModel::LOCAL_FORWARDING_POINTER_SPEC.load_atomic::<VM, u32>(
                object,
                Some(FORWARDING_POINTER_MASK),
                Ordering::SeqCst,
            ) as usize)
                << FORWARDING_ADDRESS_SHIFT,
        ))
    }
}

/// Read the potential forwarding pointer of an object. This function is used by
/// the ART binding for arbitrary addresses which may-or-may not be actual
/// objects.
pub fn read_potential_forwarding_pointer<VM: VMBinding>(object: ObjectReference) -> Address {
    debug_assert!(
        is_forwarded_or_being_forwarded::<VM>(object),
        "read_potential_forwarding_pointer called for object {:?} that has not started forwarding!",
        object,
    );

    // We write the forwarding poiner. We know it is an object reference.
    unsafe {
        crate::util::Address::from_usize(
            (VM::VMObjectModel::LOCAL_FORWARDING_POINTER_SPEC.load_atomic::<VM, u32>(
                object,
                Some(FORWARDING_POINTER_MASK),
                Ordering::SeqCst,
            ) as usize)
                << FORWARDING_ADDRESS_SHIFT,
        )
    }
}

/// Write the forwarding pointer of an object.
/// This function is called on being_forwarded objects.
pub fn write_forwarding_pointer<VM: VMBinding>(
    object: ObjectReference,
    new_object: ObjectReference,
) {
    debug_assert!(
        is_being_forwarded::<VM>(object),
        "write_forwarding_pointer called for object {:?} that is not being forwarded! Mark word = 0x{:x}",
        object,
        get_mark_word::<VM>(object),
    );

    trace!("write_forwarding_pointer({}, {})", object, new_object);
    VM::VMObjectModel::LOCAL_FORWARDING_POINTER_SPEC.store_atomic::<VM, u32>(
        object,
        (new_object.to_raw_address().as_usize() >> FORWARDING_ADDRESS_SHIFT)
            .try_into()
            .unwrap(),
        Some(FORWARDING_POINTER_MASK),
        Ordering::SeqCst,
    );

    debug_assert!(
        is_forwarded::<VM>(object),
        "write_forwarding_pointer object {:?} should be forwarded now! Mark word = 0x{:x}",
        object,
        get_mark_word::<VM>(object),
    );
}

/// (This function is only used internal to the `util` module)
///
/// This function checks whether the forwarding pointer and forwarding bits can be written in the same atomic operation.
///
/// Returns `None` if this is not possible.
/// Otherwise, returns `Some(shift)`, where `shift` is the left shift needed on forwarding bits.
///
#[cfg(target_endian = "little")]
pub(super) fn forwarding_bits_offset_in_forwarding_pointer<VM: VMBinding>() -> Option<isize> {
    use std::ops::Deref;
    // if both forwarding bits and forwarding pointer are in-header
    match (
        VM::VMObjectModel::LOCAL_FORWARDING_POINTER_SPEC.deref(),
        VM::VMObjectModel::LOCAL_FORWARDING_BITS_SPEC.deref(),
    ) {
        (MetadataSpec::InHeader(fp), MetadataSpec::InHeader(fb)) => {
            let maybe_shift = fb.bit_offset - fp.bit_offset;
            if maybe_shift >= 0 && maybe_shift < constants::BITS_IN_WORD as isize {
                Some(maybe_shift)
            } else {
                None
            }
        }
        _ => None,
    }
}

#[cfg(target_endian = "big")]
pub(super) fn forwarding_bits_offset_in_forwarding_pointer<VM: VMBinding>() -> Option<isize> {
    unimplemented!()
}
