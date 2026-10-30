import json
import os
import uuid

from sqlalchemy import select

from eventhub.contracts import CaseDefinition, checksum
from eventhub.models import APIKey, Project, ReplayCase
from eventhub.security import SCOPES, new_key
from eventhub.services import create_destination
from eventhub.settings import ROOT

DEFAULT_PROJECT = uuid.uuid5(uuid.NAMESPACE_URL, "eventhub:studio-operations")


async def bootstrap(db, config):
    private = ROOT / ".local"
    private.mkdir(mode=0o700, exist_ok=True)
    access = private / "owner-access.txt"
    raw = None
    async with db.session() as s, s.begin():
        project = await s.get(Project, DEFAULT_PROJECT)
        if not project:
            project = Project(id=DEFAULT_PROJECT, name="Studio operations")
            s.add(project)
            await s.flush()
        key = await s.scalar(select(APIKey).where(APIKey.project_id == project.id, APIKey.role == "owner"))
        if not key:
            if access.exists():
                raise RuntimeError("Private access file exists; preserve it and review database selection")
            raw, key = new_key(config, project.id, SCOPES, "owner", ["*"])
            s.add(key)
            await s.flush()
    if raw:
        content = {"project_id": str(DEFAULT_PROJECT), "key": raw, "url": "http://127.0.0.1:8100/"}
        with os.fdopen(os.open(access, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "w") as handle:
            handle.write(json.dumps(content, indent=2) + "\n")
    from eventhub.models import Destination

    for variant in ("a", "b"):
        async with db.session() as s:
            present = await s.scalar(
                select(Destination).where(
                    Destination.project_id == DEFAULT_PROJECT, Destination.adapter == "lab-" + variant
                )
            )
        if not present:
            await create_destination(db, config, key, "Receiver " + variant.upper(), "lab-" + variant)
    for file in sorted((ROOT / "lab/cases").glob("*.json")):
        definition = CaseDefinition.model_validate_json(file.read_text()).model_dump(mode="json")
        digest = checksum(definition)
        async with db.session() as s, s.begin():
            exists = await s.scalar(
                select(ReplayCase).where(
                    ReplayCase.project_id == DEFAULT_PROJECT, ReplayCase.checksum == digest
                )
            )
            if not exists:
                s.add(
                    ReplayCase(
                        project_id=DEFAULT_PROJECT,
                        name=definition["name"],
                        definition=definition,
                        checksum=digest,
                    )
                )
    return access
