#!/usr/bin/env python3
"""A stand-in for the model cartridge that answers `classify-en`.

The cartridge `capdag new` scaffolds does no inference itself: it PEER-CALLS
`classify-en`, and in a real install a BUNDLED model cartridge sitting beside the
capdag binary answers. That is the whole point of the stub — own the domain
logic, delegate the model.

Neither of those is available to `cargo test`: a test build has no bundled
cartridge tree, and a real model cartridge would download weights, want a GPU,
and make the test non-hermetic. This provides the same CAP with a deterministic
keyword rule instead of a model, and the e2e supplies it with `--dev-bins` — the
affordance that substitutes a local cartridge binary, so the host is assembled
the way production assembles it with one participant swapped.

It is NOT a dev cartridge and is never `dev-install`ed; the only dev cartridge in
that test is the scaffolded one. What it proves is the wiring — that the peer
call routes, that arguments arrive addressed by media URN, and that progress the
peer reports is forwarded — none of which depends on the answer coming from
inference.
"""

import json

from capdag.bifaci.cartridge_runtime import CartridgeRuntime, Request, WET_KEY_REQUEST
from capdag.bifaci.manifest import CapManifest, default_group
from capdag.cap.definition import Cap, CapArg, CapOutput, StdinSource
from capdag.standard.caps import CAP_IDENTITY
from capdag.urn.cap_urn import CapUrn
from ops import DryContext, Op, OpMetadata, WetContext


# The cap the scaffolded cartridge peer-calls, verbatim. If this string and the
# stub's ever diverge, the peer call stops routing and this fixture is the place
# that says so.
CLASSIFY_CAP = (
    'cap:classify;constrained;in="media:enc=utf-8";language=en;'
    'out="media:fmt=json;record;semantic-judgment"'
)

ITEM_MEDIA = "media:enc=utf-8"
LABELS_MEDIA = "media:enc=utf-8;label-set"
JUDGMENT_MEDIA = "media:fmt=json;record;semantic-judgment"

# The keyword rule standing in for the model. Deterministic, so the e2e can
# assert an exact label rather than "some label".
POSITIVE_WORDS = {"love", "good", "great", "delightful"}
NEGATIVE_WORDS = {"awful", "terrible", "bad", "hate"}


class ClassifyOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        # Two arguments, addressed by MEDIA URN rather than by position — the
        # peer-call contract. Collected separately, never concatenated.
        streams = req.take_input().collect_streams()

        item = None
        labels = None
        for media_urn, data, _meta in streams:
            if media_urn.startswith(LABELS_MEDIA):
                labels = data.decode("utf-8")
            elif media_urn.startswith(ITEM_MEDIA):
                item = data.decode("utf-8")
        if item is None:
            raise RuntimeError(
                f"no argument arrived at '{ITEM_MEDIA}'; got {[s[0] for s in streams]}"
            )
        if labels is None:
            raise RuntimeError(
                f"no argument arrived at '{LABELS_MEDIA}'; got {[s[0] for s in streams]}"
            )

        allowed = [label.strip() for label in labels.split(",") if label.strip()]
        if not allowed:
            raise RuntimeError("the label set is empty — nothing to choose from")

        emitter = req.emitter()
        emitter.start(False)
        # Report progress the way a model does while it works. The caller
        # collects this response through a FORWARDING collector, so these frames
        # are what prove the forwarding path carries them rather than rejecting
        # them.
        emitter.progress(0.5, "classifying")

        words = {w.strip(".,!?;:\"'").lower() for w in item.split()}
        if words & POSITIVE_WORDS:
            label = "positive"
        elif words & NEGATIVE_WORDS:
            label = "negative"
        else:
            label = "neutral"
        # Constrained decoding means the answer is always in the caller's label
        # set. Standing in for that here means saying so when it is not, rather
        # than emitting a label the caller declared impossible.
        if label not in allowed:
            raise RuntimeError(
                f"'{label}' is not in the caller's label set {allowed} — "
                "a constrained classifier cannot answer outside it"
            )

        emitter.finish(1.0, f"classified as {label}")
        emitter.emit_cbor(
            json.dumps(
                {
                    "label": label,
                    "confidence": 1.0,
                    "reason": "keyword rule standing in for a model",
                }
            )
        )

    def metadata(self) -> OpMetadata:
        return (
            OpMetadata.builder("ClassifyOp")
            .description("Closed-set English classification (test stand-in)")
            .build()
        )


def build_manifest() -> CapManifest:
    cap = Cap(CapUrn.from_string(CLASSIFY_CAP), "Classify (English)", ["classify-en"])
    cap.cap_description = "Label a piece of text with one of a caller-supplied label set."
    cap.args = [
        CapArg(
            media_urn=ITEM_MEDIA,
            required=True,
            sources=[StdinSource(ITEM_MEDIA)],
            arg_description="The text to classify.",
        ),
        CapArg(
            media_urn=LABELS_MEDIA,
            required=True,
            sources=[StdinSource(LABELS_MEDIA)],
            arg_description="Comma-separated labels the answer must come from.",
        ),
    ]
    cap.output = CapOutput(
        media_urn=JUDGMENT_MEDIA,
        output_description="A semantic-judgment record: label, confidence, reason.",
    )

    identity = Cap(CapUrn.from_string(CAP_IDENTITY), "Identity", ["identity"])

    return CapManifest(
        name="e2e-classifier",
        version="0.1.0",
        channel="nightly",
        registry_url=None,
        description="Closed-set English classification (test stand-in).",
        cap_groups=[default_group([identity, cap])],
    )


def main() -> None:
    runtime = CartridgeRuntime.with_manifest(build_manifest())
    runtime.register_op_type(CapUrn.from_string(CLASSIFY_CAP).to_string(), ClassifyOp)
    runtime.run()


if __name__ == "__main__":
    main()
