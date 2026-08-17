-- Standardized AIS/ITU-R M.1371 "Type of ship and cargo" categories,
-- grouped at the tens-digit level (10 categories covering the full
-- 0-99 code range). Source: ITU-R M.1371 / USCG Navigation Center AIS
-- encoding guide. Real, external, standardized reference data -- not
-- generated or captured by this pipeline.
INSERT INTO portpulse.dim_vessel_type (vessel_type_id, category_name, code_range) VALUES
    (0, 'Not available / no ship type info', '0-9'),
    (1, 'Reserved for future use', '10-19'),
    (2, 'Wing In Ground (WIG) craft', '20-29'),
    (3, 'Special craft: fishing, towing, dredging, diving, military, sailing, pleasure craft', '30-39'),
    (4, 'High speed craft (HSC)', '40-49'),
    (5, 'Special vessels: pilot, search and rescue, tug, port tender, anti-pollution, law enforcement, medical', '50-59'),
    (6, 'Passenger ships', '60-69'),
    (7, 'Cargo ships', '70-79'),
    (8, 'Tanker vessels', '80-89'),
    (9, 'Other types of ship', '90-99');
