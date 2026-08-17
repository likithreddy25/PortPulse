from ingestion.geofence import assign_port_zone


def test_center_point_assigns_correct_port():
    assert assign_port_zone(33.7, -118.2) == "USLAX"
    assert assign_port_zone(1.26, 103.8) == "SGSIN"
    assert assign_port_zone(51.9, 4.0) == "NLRTM"


def test_open_ocean_point_has_no_zone():
    assert assign_port_zone(0.0, 0.0) is None


def test_point_just_outside_bbox_has_no_zone():
    # USLAX bbox is centered at (33.7, -118.2) with 0.35/0.45 buffers
    assert assign_port_zone(33.7, -119.0) is None
