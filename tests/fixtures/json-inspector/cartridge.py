#!/usr/bin/env python3
"""A cartridge that inspects JSON files, and does nothing else.

It declares one content-inspection adapter, `media:enc=utf8;fmt=json`, as the
data cartridge does, and answers adapter selection by reading the file's
content: an object is a JSON record, an array of objects is a list of JSON
records, any other JSON is plain JSON, and what is not JSON it declines. The
file-inspection e2e installs it under the dev slot and asks the capdag command
what a .json file is: the answer is the record or the list only if the command
consulted this cartridge, since nothing else proves a JSON file is either.
"""

import json

from capdag.bifaci.cartridge_runtime import CartridgeRuntime, Request, WET_KEY_REQUEST
from capdag.bifaci.manifest import CapGroup, CapManifest
from capdag.standard.caps import CAP_ADAPTER_SELECTION, identity_cap
from ops import DryContext, Op, OpMetadata, WetContext

# What the adapter covers: every JSON file type the fabric declares.
ADAPTER_URN = "media:enc=utf8;fmt=json"
JSON = "media:enc=utf8;ext=json;fmt=json"
RECORD = "media:enc=utf8;ext=json;fmt=json;record"
LIST_OF_RECORDS = "media:enc=utf8;ext=json;fmt=json;list;record"


def claims(content: bytes) -> list:
    """What the content proves the file is, or nothing."""
    try:
        value = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return []
    if isinstance(value, dict):
        return [RECORD]
    if isinstance(value, list) and value and all(isinstance(item, dict) for item in value):
        return [LIST_OF_RECORDS]
    return [JSON]


class InspectJsonOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        streams = req.take_input().collect_streams()
        if len(streams) != 1:
            raise RuntimeError(f"adapter selection takes one stream, got {len(streams)}")
        _media, content, _meta = streams[0]
        found = claims(content)
        if not found:
            # Declining is an empty END: no stream at all.
            return
        emitter = req.emitter()
        emitter.start(False)
        emitter.emit_cbor(json.dumps({"media_urns": found}).encode("utf-8"))

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("InspectJsonOp").description("Tells a JSON record from a list of them").build()


def build_manifest() -> CapManifest:
    return CapManifest(
        name="json-inspector",
        version="0.1.0",
        channel="nightly",
        registry_url=None,
        description="Inspects JSON files (test cartridge).",
        cap_groups=[CapGroup(name="json-inspection", caps=[identity_cap()], adapter_urns=[ADAPTER_URN])],
    )


def main() -> None:
    runtime = CartridgeRuntime.with_manifest(build_manifest())
    runtime.register_op_type(CAP_ADAPTER_SELECTION, InspectJsonOp)
    runtime.run()


if __name__ == "__main__":
    main()
