from datetime import datetime

from flask import Blueprint, jsonify, request

from app.dao.date_util import get_financial_year_for_datetime
from app.dao.fact_billing_dao import (
    fetch_billing_details_for_all_services,
    fetch_letter_costs_for_all_services,
    fetch_letter_line_items_for_all_services,
    fetch_sms_billing_for_all_services,
)
from app.dao.fact_notification_status_dao import (
    fetch_notification_status_totals_for_all_services,
)
from app.errors import InvalidRequest, register_errors
from app.models import UK_POSTAGE_TYPES
from app.platform_stats.platform_stats_schema import platform_stats_request
from app.schema_validation import validate
from app.service.statistics import format_admin_stats
from app.utils import get_london_midnight_in_utc

platform_stats_blueprint = Blueprint('platform_stats', __name__)

register_errors(platform_stats_blueprint)


@platform_stats_blueprint.route('')
def get_platform_stats():
    if request.args:
        validate(request.args, platform_stats_request)

    # If start and end date are not set, we are expecting today's stats.
    today = str(datetime.utcnow().date())

    start_date = datetime.strptime(request.args.get('start_date', today), '%Y-%m-%d').date()
    end_date = datetime.strptime(request.args.get('end_date', today), '%Y-%m-%d').date()
    data = fetch_notification_status_totals_for_all_services(start_date=start_date, end_date=end_date)
    stats = format_admin_stats(data)

    return jsonify(stats)


def validate_date_range_is_within_a_financial_year(start_date, end_date):
    try:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        raise InvalidRequest(message="Input must be a date in the format: YYYY-MM-DD", status_code=400)
    if end_date < start_date:
        raise InvalidRequest(message="Start date must be before end date", status_code=400)

    start_fy = get_financial_year_for_datetime(get_london_midnight_in_utc(start_date))
    end_fy = get_financial_year_for_datetime(get_london_midnight_in_utc(end_date))

    if start_fy != end_fy:
        raise InvalidRequest(message="Date must be in a single financial year.", status_code=400)

    return start_date, end_date


@platform_stats_blueprint.route('usage-for-all-services')
@platform_stats_blueprint.route('data-for-billing-report')
def get_data_for_billing_report():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    start_date, end_date = validate_date_range_is_within_a_financial_year(start_date, end_date)

    sms_costs = fetch_sms_billing_for_all_services(start_date, end_date)
    letter_costs = fetch_letter_costs_for_all_services(start_date, end_date)
    letter_breakdown = fetch_letter_line_items_for_all_services(start_date, end_date)

    lb_by_service = [
        (lb.service_id,
         f"{lb.letters_sent} {postage_description(lb.postage)} letters at {format_letter_rate(lb.letter_rate)}")
        for lb in letter_breakdown
    ]
    combined = {}
    for s in sms_costs:
        if float(s.sms_cost) > 0:
            entry = {
                "organisation_id": str(s.organisation_id) if s.organisation_id else "",
                "organisation_name": s.organisation_name or "",
                "service_id": str(s.service_id),
                "service_name": s.service_name,
                "sms_cost": float(s.sms_cost),
                "sms_fragments": s.chargeable_billable_sms,
                "letter_cost": 0,
                "letter_breakdown": ""
            }
            combined[s.service_id] = entry

    for letter_cost in letter_costs:
        if letter_cost.service_id in combined:
            combined[letter_cost.service_id].update({'letter_cost': float(letter_cost.letter_cost)})
        else:
            letter_entry = {
                "organisation_id": str(letter_cost.organisation_id) if letter_cost.organisation_id else "",
                "organisation_name": letter_cost.organisation_name or "",
                "service_id": str(letter_cost.service_id),
                "service_name": letter_cost.service_name,
                "sms_cost": 0,
                "sms_fragments": 0,
                "letter_cost": float(letter_cost.letter_cost),
                "letter_breakdown": ""
            }
            combined[letter_cost.service_id] = letter_entry
    for service_id, breakdown in lb_by_service:
        combined[service_id]['letter_breakdown'] += (breakdown + '\n')

    billing_details = fetch_billing_details_for_all_services()
    for service in billing_details:
        if service.service_id in combined:
            combined[service.service_id].update({
                    'purchase_order_number': service.purchase_order_number,
                    'contact_names': service.billing_contact_names,
                    'contact_email_addresses': service.billing_contact_email_addresses,
                    'billing_reference': service.billing_reference
                })

    # sorting first by name == '' means that blank orgs will be sorted last.

    result = sorted(combined.values(), key=lambda x: (
        x['organisation_name'] == '',
        x['organisation_name'],
        x['service_name']
    ))
    return jsonify(result)


def postage_description(postage):
    if postage in UK_POSTAGE_TYPES:
        return f'{postage} class'
    else:
        return 'international'


def format_letter_rate(number):
    if number >= 1:
        return f"£{number:,.2f}"

    return f"{number * 100:.0f}p"
