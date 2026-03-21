export const COLORS = {
  primary: "#0066ff",
  green: "#34a86c",
  pink: "#ef5350",
  purple: "#8e5edb",
  warning: "#d97706",
  cyan: "#0891b2",
};

export const CHART_COLORS = [
  COLORS.primary,
  COLORS.purple,
  COLORS.green,
  COLORS.warning,
  COLORS.pink,
  COLORS.cyan,
];

export const STATUS_COLORS: Record<string, string> = {
  Completed: COLORS.green,
  Failed: COLORS.pink,
  TimedOut: COLORS.warning,
};

export const APPOINTMENT_PRICING: Record<string, number> = {
  "MRI": 150.00,
  "CT Scan": 125.00,
  "Ultrasound": 85.00,
  "X-Ray": 45.00,
  "Blood Work": 35.00,
  "Primary Care Visit": 75.00,
  "Specialist Consultation": 120.00,
  "Physical Therapy": 90.00,
  "Dermatology": 110.00,
  "Cardiology": 135.00,
};
