from datetime import datetime, timedelta

def generate_date_partitions(macro_start: str, macro_end: str, partition_len: int) -> list[tuple[str, str]]:
    """
    Mathematically divides a large date range into a sequential list of smaller 
    (sub_start, sub_end) date string tuples. 
    """
    date_format = "%d/%m/%Y"
    
    start_dt = datetime.strptime(macro_start, date_format)
    end_dt = datetime.strptime(macro_end, date_format)
    
    if start_dt > end_dt:
        raise ValueError(f"macro_start ({macro_start}) cannot be later than macro_end ({macro_end})")
        
    if partition_len < 1:
        raise ValueError("partition_len must be at least 1 day.")

    partitions = []
    current_start = start_dt
    
    while current_start <= end_dt:
        # Calculate the proposed end boundary (inclusive interval)
        current_end = current_start + timedelta(days=partition_len - 1)
        
        # Gracefully truncate the final tuple to end exactly on macro_end
        if current_end > end_dt:
            current_end = end_dt
            
        partitions.append(
            (current_start.strftime(date_format), current_end.strftime(date_format))
        )
        
        # Advance the pointer by exactly 1 day to prevent query overlaps
        current_start = current_end + timedelta(days=1)
        
    return partitions