CREATE TABLE years (year_id INT PRIMARY KEY);

CREATE TABLE tabletime (
    yearmonth CHAR(10) PRIMARY KEY,
    month_numb INT,
    month_name CHAR(30),
    year_id INT
    FOREIGN KEY (year_id) REFERENCES years (year_id));
    
CREATE TABLE place (
    neighborhood_id CHAR(100) PRIMARY KEY,
    neighborhood_name CHAR(100),
    district_name CHAR(100),
    city CHAR(100));
    
CREATE TABLE price_incidents (
    year_id INT,
    yearmonth CHAR(10),
    neighborhood_id CHAR(100),
    latitude double precision,
    longitude double precision,
    price double precision,
    incidents double precision,
    FOREIGN KEY (year_id) REFERENCES years (year_id),
    FOREIGN KEY (yearmonth) REFERENCES tabletime (yearmonth),
    FOREIGN KEY (neighborhood_id) REFERENCES place (neighborhood_id));
    
CREATE TABLE income (
    year_id INT,
    neighborhood_id CHAR(100),
    RFD double precision,
    FOREIGN KEY (year_id) REFERENCES years (year_id),
    FOREIGN KEY (neighborhood_id) REFERENCES place (neighborhood_id));