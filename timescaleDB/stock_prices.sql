CREATE TABLE stock_prices(  
	time BIGINT NOT NULL,  
	code TEXT NOT NULL,  
	bid_price DOUBLE PRECISION NULL, 
	ask_price DOUBLE PRECISION NULL,
	mid_price DOUBLE PRECISION NULL,
	bid_ask_spread DOUBLE PRECISION NULL,
	lagged_price DOUBLE PRECISION NULL,
	percent_change DOUBLE PRECISION NULL
); 
