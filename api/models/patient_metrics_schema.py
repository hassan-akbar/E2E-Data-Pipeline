from pydantic import BaseModel


class PatientSummary(BaseModel):
    ward: str
    bed_number: str
    pct_left: float
    pct_right: float
    pct_prone: float
    pct_supine: float
    pct_npp: float
    pct_no_data: float
    continous_on_bed_hrs: float
    no_data_count: int
    posture_changes: int
    posture_changes_per_hour: float
