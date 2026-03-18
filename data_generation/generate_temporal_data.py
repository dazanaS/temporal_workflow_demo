"""
Synthetic Temporal Workflow Data Generator for PocketHealth Demo
Generates realistic Temporal workflow output JSON files representing
scheduled healthcare appointments across Canadian facilities.
"""

import json
import random
import uuid
import os
from datetime import datetime, timedelta

# --- Configuration ---
NUM_DAYS = 30
MIN_WORKFLOWS_PER_DAY = 120
MAX_WORKFLOWS_PER_DAY = 200
OUTPUT_DIR = "output"
FAILURE_RATE = 0.03  # 3% failure rate

# --- Reference Data ---
WORKFLOW_TYPES = [
    ("ScheduleAppointmentWorkflow", 0.50),
    ("RescheduleAppointmentWorkflow", 0.15),
    ("CancelAppointmentWorkflow", 0.10),
    ("ReferralIntakeWorkflow", 0.12),
    ("FollowUpSchedulerWorkflow", 0.08),
    ("WaitlistPromotionWorkflow", 0.05),
]

APPOINTMENT_TYPES = [
    "Primary Care Visit", "MRI", "CT Scan", "Ultrasound",
    "Blood Work", "X-Ray", "Specialist Consultation",
    "Physical Therapy", "Dermatology", "Cardiology"
]

FACILITIES = [
    {"facilityId": "FAC-1001", "name": "Toronto General Hospital", "address": "200 Elizabeth St, Toronto, ON", "region": "Ontario"},
    {"facilityId": "FAC-1002", "name": "Sunnybrook Health Sciences Centre", "address": "2075 Bayview Ave, Toronto, ON", "region": "Ontario"},
    {"facilityId": "FAC-1003", "name": "Mount Sinai Hospital", "address": "600 University Ave, Toronto, ON", "region": "Ontario"},
    {"facilityId": "FAC-1004", "name": "St. Michael's Hospital", "address": "36 Queen St E, Toronto, ON", "region": "Ontario"},
    {"facilityId": "FAC-1005", "name": "Women's College Hospital", "address": "76 Grenville St, Toronto, ON", "region": "Ontario"},
    {"facilityId": "FAC-2001", "name": "Ottawa Hospital - Civic Campus", "address": "1053 Carling Ave, Ottawa, ON", "region": "Ontario"},
    {"facilityId": "FAC-2002", "name": "Kingston General Hospital", "address": "76 Stuart St, Kingston, ON", "region": "Ontario"},
    {"facilityId": "FAC-3001", "name": "McGill University Health Centre", "address": "1001 Decarie Blvd, Montreal, QC", "region": "Quebec"},
    {"facilityId": "FAC-3002", "name": "Centre hospitalier de l'Universite de Montreal", "address": "1051 Sanguinet St, Montreal, QC", "region": "Quebec"},
    {"facilityId": "FAC-4001", "name": "Vancouver General Hospital", "address": "899 W 12th Ave, Vancouver, BC", "region": "British Columbia"},
    {"facilityId": "FAC-4002", "name": "St. Paul's Hospital", "address": "1081 Burrard St, Vancouver, BC", "region": "British Columbia"},
    {"facilityId": "FAC-5001", "name": "Foothills Medical Centre", "address": "1403 29 St NW, Calgary, AB", "region": "Alberta"},
]

PROVIDERS = [
    {"providerId": "DR-1001", "name": "Dr. Amir Patel", "specialty": "Radiology"},
    {"providerId": "DR-1002", "name": "Dr. Sarah Chen", "specialty": "Family Medicine"},
    {"providerId": "DR-1003", "name": "Dr. James Wilson", "specialty": "Cardiology"},
    {"providerId": "DR-1004", "name": "Dr. Maria Santos", "specialty": "Dermatology"},
    {"providerId": "DR-1005", "name": "Dr. David Kim", "specialty": "Orthopedics"},
    {"providerId": "DR-1006", "name": "Dr. Emily Thompson", "specialty": "Internal Medicine"},
    {"providerId": "DR-1007", "name": "Dr. Robert Nguyen", "specialty": "Neurology"},
    {"providerId": "DR-1008", "name": "Dr. Lisa Dubois", "specialty": "Gastroenterology"},
    {"providerId": "DR-1009", "name": "Dr. Michael Brown", "specialty": "Oncology"},
    {"providerId": "DR-1010", "name": "Dr. Jennifer Lee", "specialty": "Physical Therapy"},
    {"providerId": "DR-1011", "name": "Dr. Hassan Ali", "specialty": "Radiology"},
    {"providerId": "DR-1012", "name": "Dr. Catherine O'Brien", "specialty": "Family Medicine"},
    {"providerId": "DR-1013", "name": "Dr. Raj Sharma", "specialty": "Cardiology"},
    {"providerId": "DR-1014", "name": "Dr. Anne-Marie Tremblay", "specialty": "Dermatology"},
    {"providerId": "DR-1015", "name": "Dr. Kevin Zhang", "specialty": "Internal Medicine"},
]

FIRST_NAMES = [
    "Emma", "Liam", "Olivia", "Noah", "Ava", "William", "Sophia", "James",
    "Isabella", "Oliver", "Mia", "Benjamin", "Charlotte", "Elijah", "Amelia",
    "Lucas", "Harper", "Mason", "Evelyn", "Logan", "Aria", "Alexander",
    "Chloe", "Ethan", "Ella", "Jacob", "Abigail", "Michael", "Emily", "Daniel",
    "Priya", "Wei", "Fatima", "Yuki", "Ahmed", "Mei", "Arjun", "Sakura",
    "Omar", "Leila", "Raj", "Ananya", "Hiroshi", "Zara", "Diego",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Chen", "Wang", "Li", "Zhang", "Liu",
    "Patel", "Singh", "Kumar", "Sharma", "Ali", "Kim", "Park", "Lee",
    "Nguyen", "Tran", "Dubois", "Tremblay", "Roy", "Gagnon", "Cote",
    "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White",
    "Thompson", "Campbell", "Stewart", "Fraser", "MacDonald", "Murray",
]

TASK_QUEUES = [
    "appointment-scheduling-queue",
    "referral-processing-queue",
    "waitlist-management-queue",
    "follow-up-scheduler-queue",
]

CONFIRMATION_METHODS = ["SMS", "Email", "Phone", "Patient Portal"]

STATUSES = ["Completed", "Failed", "TimedOut"]
FAILURE_REASONS = [
    "Provider unavailable for requested time slot",
    "Patient health card validation failed",
    "Facility capacity exceeded for requested date",
    "Referral authorization expired",
    "Waitlist slot claimed by another patient",
    "Scheduling conflict detected",
    "Insurance pre-authorization pending",
    "Patient contact information invalid",
]


def weighted_choice(choices):
    """Select from a list of (item, weight) tuples."""
    items, weights = zip(*choices)
    return random.choices(items, weights=weights, k=1)[0]


def generate_patient_id():
    return f"P-{random.randint(10000, 99999)}"


def generate_health_card():
    return f"XXXX-XXX-{random.randint(100, 999)}"


def generate_appointment_id(date_str):
    seq = random.randint(1000, 9999)
    return f"APT-{date_str.replace('-', '')}-{seq}"


def generate_workflow_record(day_date, seq_num):
    """Generate a single Temporal workflow execution record."""
    workflow_type = weighted_choice(WORKFLOW_TYPES)
    facility = random.choice(FACILITIES)
    provider = random.choice(PROVIDERS)

    # Determine status
    if random.random() < FAILURE_RATE:
        # WaitlistPromotion and Referral have higher failure rates
        if workflow_type in ("WaitlistPromotionWorkflow", "ReferralIntakeWorkflow"):
            status = random.choice(["Failed", "TimedOut"])
        else:
            status = "Failed"
    else:
        status = "Completed"

    # Workflow timing
    hour = random.randint(6, 22)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    start_time = day_date.replace(hour=hour, minute=minute, second=second)

    # Execution duration: 30s to 10min for success, longer for failures
    if status == "Completed":
        duration_seconds = random.randint(30, 600)
    elif status == "TimedOut":
        duration_seconds = random.randint(900, 1800)  # 15-30 min timeout
    else:
        duration_seconds = random.randint(5, 120)

    close_time = start_time + timedelta(seconds=duration_seconds)

    # Task queue based on workflow type
    if workflow_type == "ReferralIntakeWorkflow":
        task_queue = "referral-processing-queue"
    elif workflow_type == "WaitlistPromotionWorkflow":
        task_queue = "waitlist-management-queue"
    elif workflow_type == "FollowUpSchedulerWorkflow":
        task_queue = "follow-up-scheduler-queue"
    else:
        task_queue = "appointment-scheduling-queue"

    # Generate patient
    patient_id = generate_patient_id()
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    dob_year = random.randint(1940, 2008)
    dob_month = random.randint(1, 12)
    dob_day = random.randint(1, 28)

    # Appointment details
    appointment_type = random.choice(APPOINTMENT_TYPES)
    appt_date = day_date.date() + timedelta(days=random.randint(1, 30))
    appt_hour = random.choice([8, 9, 10, 11, 13, 14, 15, 16])
    appt_minute = random.choice([0, 15, 30, 45])
    duration_minutes = random.choice([15, 30, 45, 60, 90])

    workflow_id = f"{workflow_type.lower().replace('workflow', '')}-{patient_id.lower()}-{day_date.strftime('%Y%m%dT%H%M')}"
    run_id = str(uuid.uuid4())

    record = {
        "execution": {
            "workflowId": workflow_id,
            "runId": run_id
        },
        "type": {
            "name": workflow_type
        },
        "status": status,
        "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "closeTime": close_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "executionTime": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "historyLength": random.randint(8, 30),
        "stateTransitionCount": random.randint(4, 15),
        "taskQueue": task_queue,
        "memo": {},
        "searchAttributes": {
            "PatientRegion": facility["region"],
            "AppointmentType": appointment_type,
            "FacilityId": facility["facilityId"]
        },
        "result": {
            "appointmentId": generate_appointment_id(day_date.strftime("%Y-%m-%d")),
            "patient": {
                "patientId": patient_id,
                "firstName": first_name,
                "lastName": last_name,
                "dateOfBirth": f"{dob_year}-{dob_month:02d}-{dob_day:02d}",
                "healthCardNumber": generate_health_card()
            },
            "provider": {
                "providerId": provider["providerId"],
                "name": provider["name"],
                "specialty": provider["specialty"]
            },
            "appointment": {
                "type": appointment_type,
                "scheduledDate": appt_date.strftime("%Y-%m-%d"),
                "scheduledTime": f"{appt_hour:02d}:{appt_minute:02d}:00",
                "durationMinutes": duration_minutes,
                "facility": {
                    "facilityId": facility["facilityId"],
                    "name": facility["name"],
                    "address": facility["address"]
                }
            },
            "scheduling": {
                "requestedDate": day_date.strftime("%Y-%m-%d"),
                "confirmedDate": day_date.strftime("%Y-%m-%d") if status == "Completed" else None,
                "confirmationMethod": random.choice(CONFIRMATION_METHODS) if status == "Completed" else None,
                "reminderSent": random.choice([True, False]) if status == "Completed" else False
            }
        }
    }

    # Add failure reason for failed workflows
    if status != "Completed":
        record["result"]["failureReason"] = random.choice(FAILURE_REASONS)

    return record


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    end_date = datetime(2026, 3, 16)
    start_date = end_date - timedelta(days=NUM_DAYS - 1)

    total_records = 0

    for day_offset in range(NUM_DAYS):
        current_date = start_date + timedelta(days=day_offset)
        num_workflows = random.randint(MIN_WORKFLOWS_PER_DAY, MAX_WORKFLOWS_PER_DAY)

        # Weekends have fewer appointments
        if current_date.weekday() >= 5:
            num_workflows = int(num_workflows * 0.4)

        records = []
        for i in range(num_workflows):
            record = generate_workflow_record(current_date, i)
            records.append(record)

        # Sort by start time
        records.sort(key=lambda r: r["startTime"])

        filename = f"workflows_{current_date.strftime('%Y-%m-%d')}.json"
        filepath = os.path.join(OUTPUT_DIR, filename)

        with open(filepath, "w") as f:
            json.dump(records, f, indent=2)

        total_records += len(records)
        print(f"Generated {filename}: {len(records)} workflows")

    print(f"\nTotal: {total_records} workflow records across {NUM_DAYS} files")
    print(f"Output directory: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
