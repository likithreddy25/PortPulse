from batch.emr_jobs.geofence import ANCHORAGE_RADIUS_KM, haversine_km, in_anchorage, nearest_port_and_distance


def test_haversine_zero_distance_for_same_point():
    assert haversine_km(33.7, -118.2, 33.7, -118.2) == 0.0


def test_haversine_known_distance_la_to_sf_roughly_560km():
    # Los Angeles (34.05, -118.24) to San Francisco (37.77, -122.42)
    d = haversine_km(34.05, -118.24, 37.77, -122.42)
    assert 550 < d < 570


def test_nearest_port_at_center_is_itself_with_zero_distance():
    port_id, distance = nearest_port_and_distance(33.7, -118.2)
    assert port_id == "USLAX"
    assert distance == 0.0


def test_in_anchorage_true_close_to_port_center():
    port_id, distance = in_anchorage(33.71, -118.21)
    assert port_id == "USLAX"
    assert distance <= ANCHORAGE_RADIUS_KM


def test_in_anchorage_none_far_from_any_port():
    port_id, distance = in_anchorage(34.5, -118.2)  # ~89km from USLAX, no other port nearby
    assert port_id is None
    assert distance > ANCHORAGE_RADIUS_KM
