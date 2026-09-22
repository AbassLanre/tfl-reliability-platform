from streaming.silver_arrivals import split_unattributable, dedupe_predictions, build_arrivals, add_delay, window_reliability   # import the recipe, not the script (the __main__ guard is what makes this safe)
from datetime import datetime

def test_split_unattributable_separates_000(spark):          # pytest sees the name "spark" and hands in the fixture from conftest.py
    rows = [
        # (vehicle_id, event_type)  -- only the columns this function looks at; a test builds the minimum
        ("123", "arrival"),        # normal row       -> kept
        ("000", "arrival"),        # placeholder train -> unattributable
        ("456", "smoke_test"),     # junk event_type   -> neither (filtered out of kept, not "000" either)
    ]
    df = spark.createDataFrame(rows, ["vehicle_id", "event_type"])   # list of tuples + column names = a 3-row DataFrame, no files involved

    kept, unattributable = split_unattributable(df)

    assert kept.count() == 1                                        # assert = "this must be true, or the test fails"
    assert unattributable.count() == 1
    assert kept.first()["vehicle_id"] == "123"                       # .first() gives one Row; index it like a dict
    
    
    
    
    

def test_dedupe_collapses_platform_sigblings(spark):         
    rows = [
        # ("line_id", "vehicle_id", "naptan_id", "event_ts", "platform_name")  
        ("123", "train 123", "naptan 123", datetime(2026, 9, 8, 19, 40, 0), "platform 1"),       
        ("123", "train 123", "naptan 123", datetime(2026, 9, 8, 19, 40, 0), "platform 2"),       
        ("123", "train 123", "naptan 123", datetime(2026, 9, 8, 19, 40, 30), "platform 1"),   
    ]
    df = spark.createDataFrame(rows, ["line_id", "vehicle_id", "naptan_id", "event_ts", "platform_name"])  

    deduped = dedupe_predictions(df)

    assert deduped.count() == 2     
    seen = sorted(r["event_ts"] for r in deduped.select("event_ts").collect())
    assert seen == [datetime(2026, 9, 8, 19, 40, 0), datetime(2026, 9, 8, 19, 40, 30)]   
    
    
def test_build_arrivals(spark):         
    rows = [
        # (line_id, vehicle_id, naptan_id, event_ts, expected_arrival_ts, time_to_station)  
        ("123", "train 123", "naptan 123", datetime(2026, 9, 8, 16, 5, 0), datetime(2026, 9, 8, 16, 5, 30), 30),      
        ("123", "train 123", "naptan 123", datetime(2026, 9, 8, 16, 8, 0), datetime(2026, 9, 8, 16, 8, 30), 30),       
        ("123", "train 123", "naptan 123", datetime(2026, 9, 8, 17, 58, 0), datetime(2026, 9, 8, 17, 58, 30), 30),   
    ]
    df = spark.createDataFrame(rows, ["line_id", "vehicle_id", "naptan_id", "event_ts", "expected_arrival_ts", "time_to_station"])  

    arrivals = build_arrivals(df)
    
    assert arrivals.count() == 2
    
    
def test_add_delay(spark):         
    rows = [
        # (line_id, vehicle_id, naptan_id, event_ts, expected_arrival_ts, time_to_station)  
        ("123", "train 123", "naptan 123", datetime(2026, 9, 8, 16, 5, 0), datetime(2026, 9, 8, 16, 30, 0), 1500),      
        ("123", "train 123", "naptan 123", datetime(2026, 9, 8, 16, 10, 0), datetime(2026, 9, 8, 16, 28, 0), 1080),       
        # ("123", "train 123", "naptan 123", datetime(2026, 9, 8, 17, 58, 0), datetime(2026, 9, 8, 17, 58, 30), 30),   
    ]
    df = spark.createDataFrame(rows, ["line_id", "vehicle_id", "naptan_id", "event_ts", "expected_arrival_ts", "time_to_station"])  

    arrivals = add_delay(build_arrivals(df))
    
    assert arrivals.count() == 1
    assert arrivals.first()["delay_s"] == -120   
    
def test_completed_rule(spark):         
    rows = [
        # (line_id, vehicle_id, naptan_id, event_ts, expected_arrival_ts, time_to_station)  
        ("123", "train A", "naptan 123", datetime(2026, 9, 8, 16, 5, 0), datetime(2026, 9, 8, 16, 6, 0), 60),      
        ("123", "train B", "naptan 123", datetime(2026, 9, 8, 16, 5, 0), datetime(2026, 9, 8, 16, 6, 1), 61),       
        # ("123", "train 123", "naptan 123", datetime(2026, 9, 8, 17, 58, 0), datetime(2026, 9, 8, 17, 58, 30), 30),   
    ]
    df = spark.createDataFrame(rows, ["line_id", "vehicle_id", "naptan_id", "event_ts", "expected_arrival_ts", "time_to_station"])  

    arrivals = build_arrivals(df)
    
    result = sorted((r["vehicle_id"], r["completed"]) for r in arrivals.select("vehicle_id", "completed").collect())
    assert result == [("train A", True), ("train B", False)] 
      
      
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    