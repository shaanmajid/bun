use core::cell::Cell;
use core::ptr::NonNull;

/// Tracks remaining byte budget for a subprocess stdout/stderr pipe.
/// Dual-owned by the `Subprocess` and the pipe reader; freed when both disown it.
///
/// All mutable state is `Cell<T>` so the struct is only ever accessed via
/// `&MaxBuf` (shared, `SharedReadOnly` provenance). This is required because
/// the overflow callback fired from `on_read_bytes` re-enters via a sibling
/// `NonNull<MaxBuf>` and writes `owned_by_subprocess` — a `&mut MaxBuf` on the
/// caller's stack would be a Stacked-Borrows violation. With `Cell` the whole
/// re-entrancy path is `&MaxBuf`-only and the aliasing question disappears.
pub struct MaxBuf {
    /// The owning `Subprocess` (plus how to notify it), or `Subprocess::None`
    /// after subprocess finalize. See [`Subprocess`] — the overflow function
    /// pointer rides on the enum variant, so it is named once at the spawn site
    /// rather than stored/threaded separately at every usage.
    owned_by_subprocess: Cell<Subprocess>,
    /// `false` after pipereader finalize.
    pub owned_by_reader: Cell<bool>,
    /// If this goes negative, `on_max_buffer` is called on the subprocess.
    pub remaining_bytes: Cell<i64>,
    // (once both are cleared, it is freed)
}

/// Owning-`Subprocess` back-reference for a [`MaxBuf`], bundled with the
/// overflow dispatch so the function pointer lives on the enum instead of being
/// a separate field threaded through every `create_for_subprocess` call.
///
/// Erased (`NonNull<()>`) because the real `Subprocess` lives in `bun_runtime`,
/// a downstream crate that `bun_io` can't name; `on_overflow` recovers the
/// concrete type. The back-pointer is sound while `Owned`: the `Subprocess`
/// owns this `MaxBuf` (via `stdout_maxbuf`/`stderr_maxbuf`) and clears it
/// (→ `None`) on finalize / spawn error / overflow before it is freed.
///
/// Only constructed by the `unsafe` [`MaxBuf::create_for_subprocess`], so safe
/// code cannot forge the owner/callback pair that `on_read_bytes` dispatches on.
#[derive(Copy, Clone)]
pub enum Subprocess {
    /// The subprocess has disowned this `MaxBuf` (finalize / spawn error /
    /// overflow).
    None,
    /// Erased back-pointer + the `fn` that recovers the concrete `Subprocess`,
    /// matches this `MaxBuf` against its `stdout_maxbuf`/`stderr_maxbuf` slots to
    /// recover the stream kind, clears the matching slot, and kills the child.
    /// Dispatching from the `MaxBuf` itself (not the pipe-reader vtable) is
    /// load-bearing: when stdout/stderr is converted to a `ReadableStream` the
    /// `MaxBuf` moves to a `FileReader` whose vtable has no subprocess back-ref.
    Owned {
        ptr: NonNull<()>,
        on_overflow: unsafe fn(NonNull<()>, NonNull<MaxBuf>),
    },
}

// TODO(refactor): LIFETIMES.tsv classifies the caller fields (Subprocess.{stdout,stderr}_maxbuf,
// {Posix,Windows}BufferedReader.maxbuf) as SHARED → Option<Arc<MaxBuf>>. The fn params below
// (`ptr: &mut Option<NonNull<MaxBuf>>`, `value: Option<NonNull<MaxBuf>>`) and the hand-rolled
// heap::alloc/disowned()/destroy() refcount will not typecheck against those field types —
// reconcile by retyping to Option<Arc<MaxBuf>> and dropping destroy()/disowned().
impl MaxBuf {
    /// Single nonnull-asref projection for the dual-owner back-pointer.
    ///
    /// Type invariant: every `NonNull<MaxBuf>` reachable from a subprocess or
    /// pipe-reader slot was created by `create_for_subprocess` and stays live
    /// until both owners have disowned it and `destroy` runs. All fields are
    /// `Cell<_>`, so the shared `&MaxBuf` returned here is sufficient for every
    /// mutation path and re-entrancy through the overflow callback is sound.
    #[inline]
    fn live<'a>(this: &'a NonNull<MaxBuf>) -> &'a MaxBuf {
        // SAFETY: type invariant — see doc comment above.
        unsafe { this.as_ref() }
    }

    /// # Safety
    /// `owner` must be [`Subprocess::Owned`] whose `on_overflow` is callable with
    /// its `ptr` for as long as the created `MaxBuf` holds the subprocess slot —
    /// i.e. `ptr` must stay live until `remove_from_subprocess` clears it (on
    /// finalize, spawn error, or overflow). `on_read_bytes` invokes
    /// `on_overflow(ptr, ..)` from a safe context, so a mismatched pair or a
    /// dangling `ptr` is UB; this cannot be checked here, hence `unsafe`.
    pub unsafe fn create_for_subprocess(
        owner: Subprocess,
        ptr: &mut Option<NonNull<MaxBuf>>,
        initial: Option<i64>,
    ) {
        let Some(initial) = initial else {
            *ptr = None;
            return;
        };
        *ptr = Some(bun_core::heap::into_raw_nn(Box::new(MaxBuf {
            owned_by_subprocess: Cell::new(owner),
            owned_by_reader: Cell::new(false),
            remaining_bytes: Cell::new(initial),
        })));
    }

    fn disowned(&self) -> bool {
        matches!(self.owned_by_subprocess.get(), Subprocess::None) && !self.owned_by_reader.get()
    }

    /// Module-private teardown. Safe `fn` because the precondition is the
    /// module-level type invariant already documented on [`live`]: every
    /// `NonNull<MaxBuf>` reachable here was allocated by
    /// `create_for_subprocess`, and both call sites have just established
    /// `disowned()` (asserted below). The single `unsafe` op — reclaiming the
    /// `Box` — is wrapped at its use.
    fn destroy(this: NonNull<MaxBuf>) {
        debug_assert!(Self::live(&this).disowned());
        // SAFETY: type invariant — `this` was produced by
        // `bun_core::heap::into_raw_nn` in `create_for_subprocess` and is
        // freed exactly once (both owner flags now `false`).
        drop(unsafe { bun_core::heap::take(this.as_ptr()) });
    }

    pub fn remove_from_subprocess(ptr: &mut Option<NonNull<MaxBuf>>) {
        let Some(this_nn) = *ptr else { return };
        let this = Self::live(&this_nn);
        debug_assert!(matches!(
            this.owned_by_subprocess.get(),
            Subprocess::Owned { .. }
        ));
        this.owned_by_subprocess.set(Subprocess::None);
        *ptr = None;
        if this.disowned() {
            MaxBuf::destroy(this_nn);
        }
    }

    pub fn add_to_pipereader(value: Option<NonNull<MaxBuf>>, ptr: &mut Option<NonNull<MaxBuf>>) {
        let Some(value_nn) = value else { return };
        debug_assert!(ptr.is_none());
        *ptr = Some(value_nn);
        let v = Self::live(&value_nn);
        debug_assert!(!v.owned_by_reader.get());
        v.owned_by_reader.set(true);
    }

    pub fn remove_from_pipereader(ptr: &mut Option<NonNull<MaxBuf>>) {
        let Some(this_nn) = *ptr else { return };
        let this = Self::live(&this_nn);
        debug_assert!(this.owned_by_reader.get());
        this.owned_by_reader.set(false);
        *ptr = None;
        if this.disowned() {
            MaxBuf::destroy(this_nn);
        }
    }

    pub fn transfer_to_pipereader(
        prev: &mut Option<NonNull<MaxBuf>>,
        next: &mut Option<NonNull<MaxBuf>>,
    ) {
        if prev.is_none() {
            return;
        }
        *next = *prev;
        *prev = None;
    }

    /// Debit `bytes` from the budget; on overflow, dispatch the owning
    /// subprocess's `on_overflow` directly (which kills the child).
    ///
    /// Takes `NonNull` (not `&mut self`) because the overflow callback re-enters
    /// `remove_from_subprocess`, which writes `owned_by_subprocess` through a
    /// sibling pointer to this same allocation. With `Cell` fields a shared
    /// `&MaxBuf` is sufficient and the re-entrancy is sound; the single
    /// `unsafe` is the `NonNull → &MaxBuf` projection (the back-ref invariant:
    /// `this` is live while `owned_by_reader` is set, which every caller has
    /// just checked via `Some(maxbuf)`).
    pub fn on_read_bytes(this: NonNull<MaxBuf>, bytes: u64) {
        let this_ref = Self::live(&this);
        let delta = i64::try_from(bytes).unwrap_or(0);
        let remaining = this_ref
            .remaining_bytes
            .get()
            .checked_sub(delta)
            .unwrap_or(-1);
        this_ref.remaining_bytes.set(remaining);
        if remaining < 0 {
            if let Subprocess::Owned { ptr, on_overflow } = this_ref.owned_by_subprocess.get() {
                // SAFETY: `ptr` is the `Subprocess` that owns this `MaxBuf`
                // (set in `create_for_subprocess`, cleared in
                // `remove_from_subprocess`); the subprocess outlives its
                // `MaxBuf` slot, so the back-pointer is live while `Owned`.
                unsafe { on_overflow(ptr, this) };
            }
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Kind {
    Stdout,
    Stderr,
}

// ported from: src/io/MaxBuf.zig
