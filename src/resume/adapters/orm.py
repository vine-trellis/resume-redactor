import logging
from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    Boolean,
    ForeignKey,
    Index,
    event,
    DDL,
    text,
)
from sqlalchemy.dialects.postgresql import TSVECTOR, REAL
from sqlalchemy.sql.expression import func
from sqlalchemy.orm import registry, relationship

from resume.domain import model

logger = logging.getLogger(__name__)

mapper_registry = registry()
metadata = mapper_registry.metadata

prospect = Table(
    "student",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "uuid",
        String,
        unique=True,
        server_default=func.uuid_generate_v4(),
        nullable=False,
    ),
)

text_coordinates = Table(
    "text_coordinates",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("text", String, nullable=False),
    Column("tsv", TSVECTOR, nullable=True),
    Column("x0", REAL, nullable=False),
    Column("x1", REAL, nullable=False),
    Column("y0", REAL, nullable=False),
    Column("y1", REAL, nullable=False),
    Column("redacted", Boolean, nullable=True, server_default=text("false")),
    Column(
        "resume_id",
        Integer,
        ForeignKey("resume.id", ondelete="CASCADE"),
    ),
    Index("ix_text_coordinates_tsv", "tsv", postgresql_using="gin"),
)

text_coordinates_tsv_trigger = DDL(
    """
    CREATE TRIGGER text_coordinate_tsv_update BEFORE INSERT OR UPDATE
    ON text_coordinates FOR EACH ROW EXECUTE PROCEDURE
    tsvector_update_trigger(tsv, 'pg_catalog.english', text);
    """
)
event.listen(
    text_coordinates,
    "after_create",
    text_coordinates_tsv_trigger.execute_if(dialect="postgresql"),
)
resume = Table(
    "resume",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "uuid",
        String,
        unique=True,
        server_default=func.uuid_generate_v4(),
        nullable=False,
    ),
    Column("redacted_link", String, nullable=True),
    Column("redacted_text", String, nullable=True),
    Column("redacted_tsv", TSVECTOR),
    Column("skip_redaction", Boolean),
    Column("show_redacted", Boolean, server_default="true"),
    Column("link", String, nullable=False),
    Column("text", String, nullable=False),
    Column("tsv", TSVECTOR),
    Column("width", Integer),
    Column("height", Integer),
    Column("redaction_version", Integer),
    Index("ix_resume_tsv", "tsv", postgresql_using="gin"),
    Index("ix_resume_redacted_tsv", "redacted_tsv", postgresql_using="gin"),
)

resume_tsv_trigger = DDL(
    """
    CREATE TRIGGER resume_tsv_update BEFORE INSERT OR UPDATE
    ON resume FOR EACH ROW EXECUTE PROCEDURE
    tsvector_update_trigger(tsv, 'pg_catalog.english', text);
    """
)

event.listen(
    resume, "after_create", resume_tsv_trigger.execute_if(dialect="postgresql")
)

resume_redacted_tsv_trigger = DDL(
    """
    CREATE TRIGGER resume_redacted_tsv_update BEFORE INSERT OR UPDATE
    ON resume FOR EACH ROW EXECUTE PROCEDURE
    tsvector_update_trigger(redacted_tsv, 'pg_catalog.english', redacted_text);
    """
)

event.listen(
    resume, "after_create", resume_redacted_tsv_trigger.execute_if(dialect="postgresql")
)


def start_mappers():
    logger.info("Starting resume mappers")
    prospect_mapper = mapper_registry.map_imperatively(model.Prospect, prospect)
    text_coordinate_mapper = mapper_registry.map_imperatively(
        model.TextCoordinates, text_coordinates
    )
    resume_mapper = mapper_registry.map_imperatively(
        model.Resume,
        resume,
        properties={
            # "prospect_id": resume.c.student_id,
            "text_coordinates": relationship(text_coordinate_mapper),
        },
    )
    logger.info("Finished resume mappers")


@event.listens_for(model.Resume, "load")
def receive_load(resume, _):
    resume.events = []
