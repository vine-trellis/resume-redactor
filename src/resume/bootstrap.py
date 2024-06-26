from typing import Callable
import inspect
from resume.adapters.orm import start_mappers
from resume.service_layer import handlers, unit_of_work, messagebus
from resume.config import get_resume_s3_config

DEFAULT_AWS_REGION = get_resume_s3_config()["aws_region"]
DEFAULT_BUCKET = get_resume_s3_config()["bucket"]
DEFAULT_PREFIX = get_resume_s3_config()["prefix"]


def bootstrap(
    start_orm: bool = True,
    uow: unit_of_work.AbstractUnitOfWork = unit_of_work.SqlAlchemyUnitOfWork(),
):

    if start_orm:
        start_mappers()

    dependencies = {"uow": uow}
    injected_event_handlers = {
        event_type: [
            inject_dependencies(handler, dependencies) for handler in event_handlers
        ]
        for event_type, event_handlers in handlers.EVENT_HANDLERS.items()
    }
    injected_command_handlers = {
        command_type: inject_dependencies(handler, dependencies)
        for command_type, handler in handlers.COMMAND_HANDLERS.items()
    }

    return messagebus.MessageBus(
        uow=uow,
        event_handlers=injected_event_handlers,
        command_handlers=injected_command_handlers,
    )

def inject_dependencies(handler, dependencies):
    params = inspect.signature(handler).parameters
    deps = {
        name: dependency for name, dependency in dependencies.items() if name in params
    }
    injected_dep = lambda message: handler(message, **deps)
    injected_dep._original_name = handler.__name__
    return injected_dep