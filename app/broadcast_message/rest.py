from datetime import datetime

import iso8601
from flask import Blueprint, current_app, jsonify, request

from app.broadcast_message.broadcast_message_schema import (
    create_broadcast_message_schema,
    update_broadcast_message_schema,
    update_broadcast_message_status_schema,
)
from app.celery.broadcast_message_tasks import send_broadcast_event
from app.config import QueueNames
from app.dao.broadcast_message_dao import (
    dao_get_broadcast_message_by_id_and_service_id,
    dao_get_broadcast_messages_for_service,
)
from app.dao.dao_utils import dao_save_object
from app.dao.services_dao import dao_fetch_service_by_id
from app.dao.templates_dao import dao_get_template_by_id_and_service_id
from app.dao.users_dao import get_user_by_id
from app.errors import InvalidRequest, register_errors
from app.models import (
    BroadcastEvent,
    BroadcastEventMessageType,
    BroadcastMessage,
    BroadcastStatusType,
)
from app.schema_validation import validate

broadcast_message_blueprint = Blueprint(
    'broadcast_message',
    __name__,
    url_prefix='/service/<uuid:service_id>/broadcast-message'
)
register_errors(broadcast_message_blueprint)


def _parse_nullable_datetime(dt):
    if dt:
        return iso8601.parse_date(dt).replace(tzinfo=None)
    return dt


def _update_broadcast_message(broadcast_message, new_status, updating_user):
    if updating_user not in broadcast_message.service.users:
        #  we allow platform admins to cancel broadcasts
        if not (new_status == BroadcastStatusType.CANCELLED and updating_user.platform_admin):
            raise InvalidRequest(
                f'User {updating_user.id} cannot update broadcast_message {broadcast_message.id} from other service',
                status_code=400
            )

    if new_status not in BroadcastStatusType.ALLOWED_STATUS_TRANSITIONS[broadcast_message.status]:
        raise InvalidRequest(
            f'Cannot move broadcast_message {broadcast_message.id} from {broadcast_message.status} to {new_status}',
            status_code=400
        )

    if new_status == BroadcastStatusType.BROADCASTING:
        # training mode services can approve their own broadcasts
        if updating_user == broadcast_message.created_by and not broadcast_message.service.restricted:
            raise InvalidRequest(
                f'User {updating_user.id} cannot approve their own broadcast_message {broadcast_message.id}',
                status_code=400
            )
        elif len(broadcast_message.areas['simple_polygons']) == 0:
            raise InvalidRequest(
                f'broadcast_message {broadcast_message.id} has no selected areas and so cannot be broadcasted.',
                status_code=400
            )
        else:
            broadcast_message.approved_at = datetime.utcnow()
            broadcast_message.approved_by = updating_user

    if new_status == BroadcastStatusType.CANCELLED:
        broadcast_message.cancelled_at = datetime.utcnow()
        broadcast_message.cancelled_by = updating_user

    current_app.logger.info(
        f'broadcast_message {broadcast_message.id} moving from {broadcast_message.status} to {new_status}'
    )
    broadcast_message.status = new_status


@broadcast_message_blueprint.route('', methods=['GET'])
def get_broadcast_messages_for_service(service_id):
    # TODO: should this return template content/data in some way? or can we rely on them being cached admin side.
    # we might need stuff like template name for showing on the dashboard.
    # TODO: should this paginate or filter on dates or anything?
    broadcast_messages = [o.serialize() for o in dao_get_broadcast_messages_for_service(service_id)]
    return jsonify(broadcast_messages=broadcast_messages)


@broadcast_message_blueprint.route('/<uuid:broadcast_message_id>', methods=['GET'])
def get_broadcast_message(service_id, broadcast_message_id):
    return jsonify(dao_get_broadcast_message_by_id_and_service_id(broadcast_message_id, service_id).serialize())


@broadcast_message_blueprint.route('', methods=['POST'])
def create_broadcast_message(service_id):
    data = request.get_json()

    validate(data, create_broadcast_message_schema)
    service = dao_fetch_service_by_id(data['service_id'])
    user = get_user_by_id(data['created_by'])
    personalisation = data.get('personalisation', {})
    template_id = data.get('template_id')

    if template_id:
        template = dao_get_template_by_id_and_service_id(
            template_id, data['service_id']
        )
        content = template._as_utils_template_with_personalisation(
            personalisation
        ).content_with_placeholders_filled_in
        reference = None
    else:
        template, content, reference = None, data['content'], data['reference']

    broadcast_message = BroadcastMessage(
        service_id=service.id,
        template_id=template_id,
        template_version=template.version if template else None,
        personalisation=personalisation,
        areas={"areas": data.get("areas", []), "simple_polygons": data.get("simple_polygons", [])},
        status=BroadcastStatusType.DRAFT,
        starts_at=_parse_nullable_datetime(data.get('starts_at')),
        finishes_at=_parse_nullable_datetime(data.get('finishes_at')),
        created_by_id=user.id,
        content=content,
        reference=reference,
        stubbed=service.restricted
    )

    dao_save_object(broadcast_message)

    return jsonify(broadcast_message.serialize()), 201


@broadcast_message_blueprint.route('/<uuid:broadcast_message_id>', methods=['POST'])
def update_broadcast_message(service_id, broadcast_message_id):
    data = request.get_json()

    validate(data, update_broadcast_message_schema)

    broadcast_message = dao_get_broadcast_message_by_id_and_service_id(broadcast_message_id, service_id)

    if broadcast_message.status not in BroadcastStatusType.PRE_BROADCAST_STATUSES:
        raise InvalidRequest(
            f'Cannot update broadcast_message {broadcast_message.id} while it has status {broadcast_message.status}',
            status_code=400
        )

    if ('areas' in data and 'simple_polygons' not in data) or ('areas' not in data and 'simple_polygons' in data):
        raise InvalidRequest(
            f'Cannot update broadcast_message {broadcast_message.id}, areas or polygons are missing.',
            status_code=400
        )

    if 'personalisation' in data:
        broadcast_message.personalisation = data['personalisation']
    if 'starts_at' in data:
        broadcast_message.starts_at = _parse_nullable_datetime(data['starts_at'])
    if 'finishes_at' in data:
        broadcast_message.finishes_at = _parse_nullable_datetime(data['finishes_at'])
    if 'areas' in data and 'simple_polygons' in data:
        broadcast_message.areas = {"areas": data["areas"], "simple_polygons": data["simple_polygons"]}

    dao_save_object(broadcast_message)

    return jsonify(broadcast_message.serialize()), 200


@broadcast_message_blueprint.route('/<uuid:broadcast_message_id>/status', methods=['POST'])
def update_broadcast_message_status(service_id, broadcast_message_id):
    data = request.get_json()

    validate(data, update_broadcast_message_status_schema)
    broadcast_message = dao_get_broadcast_message_by_id_and_service_id(broadcast_message_id, service_id)

    new_status = data['status']
    updating_user = get_user_by_id(data['created_by'])

    _update_broadcast_message(broadcast_message, new_status, updating_user)
    dao_save_object(broadcast_message)

    if new_status in {BroadcastStatusType.BROADCASTING, BroadcastStatusType.CANCELLED}:
        _create_broadcast_event(broadcast_message)

    return jsonify(broadcast_message.serialize()), 200


def _create_broadcast_event(broadcast_message):
    """
    Creates a broadcast event, stores it in the database, and triggers the task to send the CAP XML off
    """
    msg_types = {
        BroadcastStatusType.BROADCASTING: BroadcastEventMessageType.ALERT,
        BroadcastStatusType.CANCELLED: BroadcastEventMessageType.CANCEL,
    }

    event = BroadcastEvent(
        service=broadcast_message.service,
        broadcast_message=broadcast_message,
        message_type=msg_types[broadcast_message.status],
        transmitted_content={"body": broadcast_message.content},
        transmitted_areas=broadcast_message.areas,
        # TODO: Probably move this somewhere more standalone too and imply that it shouldn't change. Should it include
        # a service based identifier too? eg "flood-warnings@notifications.service.gov.uk" or similar
        transmitted_sender='notifications.service.gov.uk',

        # TODO: Should this be set to now? Or the original starts_at?
        transmitted_starts_at=broadcast_message.starts_at,
        transmitted_finishes_at=broadcast_message.finishes_at,
    )

    dao_save_object(event)

    if not broadcast_message.stubbed or current_app.config['NOTIFY_ENVIRONMENT'] in ['preview', 'development']:
        send_broadcast_event.apply_async(
            kwargs={'broadcast_event_id': str(event.id)},
            queue=QueueNames.BROADCASTS
        )
