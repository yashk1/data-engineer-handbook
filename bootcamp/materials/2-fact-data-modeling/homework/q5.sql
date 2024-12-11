-- 5. A DDL for hosts_cumulated table 
-- a host_activity_datelist which logs to see which dates each host is experiencing any activity

CREATE TABLE HOSTS_CUMULATED (
	HOST TEXT,
	date DATE,
	HOST_ACTIVITY_DATELIST DATE[],
	PRIMARY KEY(HOST, date)
);
