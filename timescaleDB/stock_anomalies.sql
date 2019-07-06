CREATE TABLE stock_anomalies(  
	time bigint NOT NULL,  
	code TEXT NOT NULL,  
	percent_change DOUBLE PRECISION NULL,
	num_of_std_away DOUBLE PRECISION NULL
); 
