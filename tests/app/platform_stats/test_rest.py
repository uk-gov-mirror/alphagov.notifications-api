from datetime import date, datetime

import pytest
from freezegun import freeze_time

from app.errors import InvalidRequest
from app.models import EMAIL_TYPE, SMS_TYPE
from app.platform_stats.rest import (
    validate_date_range_is_within_a_financial_year,
)
from tests.app.db import (
    create_ft_notification_status,
    create_notification,
    create_service,
    create_template,
    set_up_usage_data,
)


@freeze_time('2018-06-01')
def test_get_platform_stats_uses_todays_date_if_no_start_or_end_date_is_provided(admin_request, mocker):
    today = datetime.now().date()
    dao_mock = mocker.patch('app.platform_stats.rest.fetch_notification_status_totals_for_all_services')
    mocker.patch('app.service.rest.statistics.format_statistics')

    admin_request.get('platform_stats.get_platform_stats')

    dao_mock.assert_called_once_with(start_date=today, end_date=today)


def test_get_platform_stats_can_filter_by_date(admin_request, mocker):
    start_date = date(2017, 1, 1)
    end_date = date(2018, 1, 1)
    dao_mock = mocker.patch('app.platform_stats.rest.fetch_notification_status_totals_for_all_services')
    mocker.patch('app.service.rest.statistics.format_statistics')

    admin_request.get('platform_stats.get_platform_stats', start_date=start_date, end_date=end_date)

    dao_mock.assert_called_once_with(start_date=start_date, end_date=end_date)


def test_get_platform_stats_validates_the_date(admin_request):
    start_date = '1234-56-78'

    response = admin_request.get(
        'platform_stats.get_platform_stats', start_date=start_date,
        _expected_status=400
    )

    assert response['errors'][0]['message'] == 'start_date time data {} does not match format %Y-%m-%d'.format(
        start_date)


@freeze_time('2018-10-31 14:00')
def test_get_platform_stats_with_real_query(admin_request, notify_db_session):
    service_1 = create_service(service_name='service_1')
    sms_template = create_template(service=service_1, template_type=SMS_TYPE)
    email_template = create_template(service=service_1, template_type=EMAIL_TYPE)
    create_ft_notification_status(date(2018, 10, 29), 'sms', service_1, count=10)
    create_ft_notification_status(date(2018, 10, 29), 'email', service_1, count=3)

    create_notification(sms_template, created_at=datetime(2018, 10, 31, 11, 0, 0), key_type='test')
    create_notification(sms_template, created_at=datetime(2018, 10, 31, 12, 0, 0), status='delivered')
    create_notification(email_template, created_at=datetime(2018, 10, 31, 13, 0, 0), status='delivered')

    response = admin_request.get(
        'platform_stats.get_platform_stats', start_date=date(2018, 10, 29),
    )
    assert response == {
        'email': {
            'failures': {
                'virus-scan-failed': 0, 'temporary-failure': 0, 'permanent-failure': 0, 'technical-failure': 0},
            'total': 4, 'test-key': 0
        },
        'letter': {
            'failures': {
                'virus-scan-failed': 0, 'temporary-failure': 0, 'permanent-failure': 0, 'technical-failure': 0},
            'total': 0, 'test-key': 0
        },
        'sms': {
            'failures': {
                'virus-scan-failed': 0, 'temporary-failure': 0, 'permanent-failure': 0, 'technical-failure': 0},
            'total': 11, 'test-key': 1
        }
    }


@pytest.mark.parametrize('start_date, end_date',
                         [('2019-04-01', '2019-06-30'),
                          ('2019-08-01', '2019-09-30'),
                          ('2019-01-01', '2019-03-31'),
                          ('2019-12-01', '2020-02-28')])
def test_validate_date_range_is_within_a_financial_year(start_date, end_date):
    validate_date_range_is_within_a_financial_year(start_date, end_date)


@pytest.mark.parametrize('start_date, end_date',
                         [('2019-04-01', '2020-06-30'),
                          ('2019-01-01', '2019-04-30'),
                          ('2019-12-01', '2020-04-30'),
                          ('2019-03-31', '2019-04-01')])
def test_validate_date_range_is_within_a_financial_year_raises(start_date, end_date):
    with pytest.raises(expected_exception=InvalidRequest) as e:
        validate_date_range_is_within_a_financial_year(start_date, end_date)
    assert e.value.message == 'Date must be in a single financial year.'
    assert e.value.status_code == 400


def test_validate_date_is_within_a_financial_year_raises_validation_error():
    start_date = '2019-08-01'
    end_date = '2019-06-01'

    with pytest.raises(expected_exception=InvalidRequest) as e:
        validate_date_range_is_within_a_financial_year(start_date, end_date)
    assert e.value.message == 'Start date must be before end date'
    assert e.value.status_code == 400


@pytest.mark.parametrize('start_date, end_date',
                         [('22-01-2019', '2019-08-01'),
                          ('2019-07-01', 'not-date')])
def test_validate_date_is_within_a_financial_year_when_input_is_not_a_date(start_date, end_date):
    with pytest.raises(expected_exception=InvalidRequest) as e:
        validate_date_range_is_within_a_financial_year(start_date, end_date)
    assert e.value.message == 'Input must be a date in the format: YYYY-MM-DD'
    assert e.value.status_code == 400


def test_get_data_for_billing_report(notify_db_session, admin_request):
    fixtures = set_up_usage_data(datetime(2019, 5, 1))
    response = admin_request.get(
        "platform_stats.get_data_for_billing_report",
        start_date='2019-05-01',
        end_date='2019-06-30'
    )

    # we set up 6 services, but only 4 returned. service_with_emails was skipped as it had no bills to pay,
    # and likewise the service with SMS within allowance was skipped. too.
    assert len(response) == 4
    assert response[0]["organisation_id"] == str(fixtures["org_1"].id)
    assert response[0]["service_id"] == str(fixtures["service_1_sms_and_letter"].id)
    assert response[0]["sms_cost"] == 0
    assert response[0]["sms_fragments"] == 0
    assert response[0]["letter_cost"] == 3.40
    assert response[0]["letter_breakdown"] == "6 second class letters at 45p\n2 first class letters at 35p\n"
    assert response[0]["purchase_order_number"] == "service purchase order number"
    assert response[0]["contact_names"] == "service billing contact names"
    assert response[0]["contact_email_addresses"] == "service@billing.contact email@addresses.gov.uk"
    assert response[0]["billing_reference"] == "service billing reference"

    assert response[1]["organisation_id"] == str(fixtures["org_for_service_with_letters"].id)
    assert response[1]["service_id"] == str(fixtures["service_with_letters"].id)
    assert response[1]["sms_cost"] == 0
    assert response[1]["sms_fragments"] == 0
    assert response[1]["letter_cost"] == 14
    assert response[1]["letter_breakdown"] == "20 second class letters at 65p\n2 first class letters at 50p\n"
    assert response[1]["purchase_order_number"] == "org3 purchase order number"
    assert response[1]["contact_names"] == "org3 billing contact names"
    assert response[1]["contact_email_addresses"] == "org3@billing.contact email@addresses.gov.uk"
    assert response[1]["billing_reference"] == "org3 billing reference"

    assert response[2]["organisation_id"] == ""
    assert response[2]["service_id"] == str(fixtures["service_with_sms_without_org"].id)
    assert response[2]["sms_cost"] == 0.33
    assert response[2]["sms_fragments"] == 3
    assert response[2]["letter_cost"] == 0
    assert response[2]["letter_breakdown"] == ""
    assert response[2]["purchase_order_number"] == "sms purchase order number"
    assert response[2]["contact_names"] == "sms billing contact names"
    assert response[2]["contact_email_addresses"] == "sms@billing.contact email@addresses.gov.uk"
    assert response[2]["billing_reference"] == "sms billing reference"

    assert response[3]["organisation_id"] == ""
    assert response[3]["service_id"] == str(fixtures["service_with_letters_without_org"].id)
    assert response[3]["sms_cost"] == 0
    assert response[3]["sms_fragments"] == 0
    assert response[3]["letter_cost"] == 24.45
    assert response[3]["letter_breakdown"] == (
        "2 second class letters at 35p\n1 first class letters at 50p\n15 international letters at £1.55\n"
    )
    assert response[3]["purchase_order_number"] is None
