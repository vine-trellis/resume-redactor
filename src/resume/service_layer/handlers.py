from uuid import uuid4
from typing import Callable
from common.adapters.file_store import AbstractFileStore
from resume.domain import commands, events, model, redaction
from resume.service_layer import unit_of_work
from resume.config import get_current_redaction_version

CURRENT_REDACTION_VERSION = get_current_redaction_version()


def create_resume(
    cmd: commands.CreateResume,
    uow: unit_of_work.AbstractUnitOfWork,
    file_store: AbstractFileStore,
):
    with uow:
        prospect_id = uow.prospects.get_by_uuid(cmd.prospect_uuid).id
        link = file_store.write(f"{cmd.uuid}.pdf", cmd.resume_bytes)
        resume = model.Resume.from_bytes(
            bytes=cmd.resume_bytes,
            link=link,
            # prospect_id=prospect_id,
            uuid=cmd.uuid,
        )
        uow.resumes.add(resume)
        uow.commit()

        resume.events.append(events.ResumeCreated(uuid=resume.uuid))


def attach_text_coordinates(
    cmd: commands.AttachTextCoordinates,
    uow: unit_of_work.AbstractUnitOfWork,
    file_store: AbstractFileStore,
):
    with uow:
        resume = uow.resumes.get_by_uuid(cmd.uuid)
        resume_bytes = file_store.read(resume.link)
        text_coordinates = model.find_text_coordinates(
            bytes=resume_bytes, resume_id=resume.id
        )
        resume.set_text_coordinates(text_coordinates)
        uow.resumes.add(resume)
        uow.commit()


def attach_redacted_text_coordinates(
    evt: events.ResumeRedacted,
    uow: unit_of_work.AbstractUnitOfWork,
    file_store: AbstractFileStore,
):
    with uow:
        resume = uow.resumes.get_by_uuid(evt.uuid)
        redacted_resume_bytes = file_store.read(resume.redacted_link)
        text_coordinates = model.find_text_coordinates(
            bytes=redacted_resume_bytes, resume_id=resume.id, redacted=True
        )
        resume.add_text_coordinates(text_coordinates)
        uow.resumes.add(resume)
        uow.commit()


def kickoff_resume_redaction(
    cmd: commands.KickoffResumeRedaction,
    uow: unit_of_work.AbstractUnitOfWork,
):
    with uow:
        resume = uow.resumes.get_without_redacted()
        if resume is not None:
            resume.skip_redaction = True
            uow.resumes.add(resume)
            uow.commit()
            resume.events.append(commands.RedactResume(uuid=resume.uuid))


def queue_redaction(evt: events.ResumeCreated, publish: Callable):
    payload = {"event": "RedactResume", "uuid": evt.uuid}
    publish("resume.redact_resume", {"uuid": evt.uuid}, wait_for_result=False)


def queue_text_coordinating(evt: events.ResumeCreated, publish: Callable):
    publish("resume.attach_text_coordinates", {"uuid": evt.uuid}, wait_for_result=False)


def redact_resume(
    cmd: commands.RedactResume,
    uow: unit_of_work.AbstractUnitOfWork,
    file_store: AbstractFileStore,
):
    with uow:
        resume = uow.resumes.get_by_uuid(cmd.uuid)
        try:
            dirty_bytes = file_store.read(resume.link)
            redacted_bytes = model.redact_pdf(
                bytes=dirty_bytes,
                redaction_strategies=[
                    redaction.Top30Percent(),
                    redaction.Bottom10Percent(),
                    redaction.ImageRedactor(),
                    redaction.LinkRedactor(),
                    redaction.MetadataRedactor(),
                ],
            )
            redacted_text = model.parse_resume_text(redacted_bytes)
            redacted_link = file_store.write(f"{str(uuid4())}.pdf", redacted_bytes)
            resume.redacted_text = redacted_text
            resume.redacted_link = redacted_link
            resume.events.append(events.ResumeRedacted(uuid=resume.uuid))
        except Exception:
            raise
        finally:
            resume.redaction_version = CURRENT_REDACTION_VERSION
            resume.skip_redaction = True
            uow.resumes.add(resume)
            uow.commit()


EVENT_HANDLERS = {
    events.ResumeCreated: [queue_redaction, queue_text_coordinating],
    events.ResumeRedacted: [attach_redacted_text_coordinates],
}
COMMAND_HANDLERS = {
    commands.CreateResume: create_resume,
    commands.AttachTextCoordinates: attach_text_coordinates,
    commands.RedactResume: redact_resume,
    commands.KickoffResumeRedaction: kickoff_resume_redaction,
}
