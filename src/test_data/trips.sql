INSERT INTO "gtfs_feed_info" ("feed_index","feed_start_date","feed_end_date")
VALUES
    ( 7, '2017-04-08', '2017-07-01'),
    (23, '2017-04-08', '2017-07-01');

INSERT INTO "gtfs_agency" ("feed_index","agency_timezone")
VALUES (7, 'America/New_York'),
    (23, 'America/New_York');

INSERT INTO "gtfs_calendar" ("feed_index","service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date")
VALUES
(7,'QV_B7-Weekday-SDon',1,1,1,1,1,0,0,'2017-04-10','2017-06-30'),
(7,'QV_B7-Saturday',0,0,0,0,0,1,0,'2017-04-15','2017-07-01'),
(23,E'UP_B7-Weekday-SDon',1,1,1,1,1,0,0,E'2017-04-19',E'2017-06-30');

INSERT INTO "gtfs_trips"
    ("feed_index","route_id","service_id","trip_id","trip_headsign","direction_id","block_id","shape_id")
VALUES
(7,E'Q1',E'QV_B7-Saturday',E'QV_B7-Saturday-035500_MISC_120',E'JAMAICA 165 ST TERM',1,NULL,E'Q010152'),
(7,E'Q1',E'QV_B7-Saturday',E'QV_B7-Saturday-038500_MISC_120',E'BELLROSE 243 ST',0,NULL,E'Q010153'),
(7,E'Q1',E'QV_B7-Saturday',E'QV_B7-Saturday-040500_MISC_113',E'QUEENS VILL JAMAICA AV',0,NULL,E'Q010154'),
(7,E'Q1',E'QV_B7-Saturday',E'QV_B7-Saturday-041800_MISC_120',E'JAMAICA 165 ST TERM',1,NULL,E'Q010152'),
(7,E'Q1',E'QV_B7-Saturday',E'QV_B7-Saturday-045500_MISC_120',E'BELLROSE 243 ST',0,NULL,E'Q010153'),
(7,E'Q1',E'QV_B7-Saturday',E'QV_B7-Saturday-049300_MISC_120',E'JAMAICA 165 ST TERM',1,NULL,E'Q010150'),
(7,E'Q1',E'QV_B7-Saturday',E'QV_B7-Saturday-053000_MISC_120',E'QUEENS VILL JAMAICA AV',0,NULL,E'Q010154'),
(7,E'Q1',E'QV_B7-Saturday',E'QV_B7-Saturday-056800_MISC_120',E'JAMAICA 165 ST TERM',1,NULL,E'Q010152'),
(7,E'Q1',E'QV_B7-Saturday',E'QV_B7-Saturday-060500_MISC_120',E'BELLROSE 243 ST',0,NULL,E'Q010153'),
(7,E'Q27',E'QV_B7-Weekday-SDon',E'QV_B7-Weekday-SDon-145500_MISC_320',E'FLUSHING  MAIN ST STA',0,NULL,E'Q270863'),
(7,E'Q27',E'QV_B7-Weekday-SDon',E'QV_B7-Weekday-SDon-147500_MISC_323',E'FLUSHING  MAIN ST STA',0,NULL,E'Q270863'),
(7,E'Q27',E'QV_B7-Weekday-SDon',E'QV_B7-Weekday-SDon-150900_MISC_320',E'CAMBRIA HTS 120 AV',1,NULL,E'Q270870'),
(7,E'Q83',E'QV_B7-Saturday',E'QV_B7-Saturday-091500_MISC_218',E'JAMAICA HILLSIDE - 153 via LIBERTY',1,NULL,E'Q830167'),
(7,E'Q83',E'QV_B7-Saturday',E'QV_B7-Saturday-095500_MISC_218',E'227 ST 114 AV via LIBERTY',0,NULL,E'Q830161'),
(7,E'Q83',E'QV_B7-Saturday',E'QV_B7-Saturday-099500_MISC_218',E'JAMAICA HILLSIDE - 153 via LIBERTY',1,NULL,E'Q830167'),
(7,E'Q83',E'QV_B7-Saturday',E'QV_B7-Saturday-103500_MISC_218',E'227 ST 114 AV via LIBERTY',0,NULL,E'Q830161'),
(7,E'Q83',E'QV_B7-Saturday',E'QV_B7-Saturday-107500_MISC_218',E'JAMAICA HILLSIDE - 153 via LIBERTY',1,NULL,E'Q830167'),
(7,E'Q83',E'QV_B7-Saturday',E'QV_B7-Saturday-111500_MISC_218',E'227 ST 114 AV via LIBERTY',0,NULL,E'Q830161'),
(7,E'Q88',E'QV_B7-Saturday',E'QV_B7-Saturday-063500_MISC_159',E'QUEENS VILL JAMAICA AV',0,NULL,E'Q880188'),
(7,E'Q88',E'QV_B7-Saturday',E'QV_B7-Saturday-067600_MISC_120',E'WOODHVN BL STA QNS CTR MALL via H.HRDNG',1,NULL,E'Q880189'),
(7,E'Q88',E'QV_B7-Saturday',E'QV_B7-Saturday-068600_MISC_159',E'WOODHVN BL STA QNS CTR MALL via H.HRDNG',1,NULL,E'Q880189'),
(7,E'Q88',E'QV_B7-Saturday',E'QV_B7-Saturday-072800_MISC_120',E'QUEENS VILL JAMAICA AV',0,NULL,E'Q880188'),
(7,E'Q88',E'QV_B7-Saturday',E'QV_B7-Saturday-078000_MISC_120',E'WOODHVN BL STA QNS CTR MALL via H.HRDNG',1,NULL,E'Q880189'),
(7,E'Q88',E'QV_B7-Saturday',E'QV_B7-Saturday-083300_MISC_120',E'QUEENS VILL JAMAICA AV',0,NULL,E'Q880188'),
(23,'B74',E'UP_B7-Weekday-SDon',E'UP_B7-Weekday-SDon-119500_B74_605',E'STILLWELL AV',1,NULL,E'B740030');
