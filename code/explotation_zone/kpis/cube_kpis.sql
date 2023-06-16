CREATE TABLE years (year_id INT PRIMARY KEY);

CREATE TABLE tabletime (
    yearmonth CHAR(7) PRIMARY KEY,
    month_numb INT,
    month_name CHAR(20),
    year_id INT);
    
CREATE TABLE place (
    neighborhood_id CHAR(50) PRIMARY KEY,
    neighborhood_name CHAR(50),
    district_name CHAR(50),
    city CHAR(50));
    
CREATE TABLE price_incidents (
    year_id INT,
    yearmonth CHAR(7),
    neighborhood_id CHAR(50),
    bathrooms INT,
    distance INT,
    exterior CHAR(50),
    floor CHAR(50),
    latitude double precision,
    longitude double precision,
    price NUMERIC(10, 1),
    priceByArea NUMERIC(10, 1),
    propertyType CHAR(50),
    rooms INT,
    size NUMERIC(10, 1),
    status CHAR(50),
    incidents INT,
    FOREIGN KEY (year_id) REFERENCES years (year_id),
    FOREIGN KEY (yearmonth) REFERENCES tabletime (yearmonth),
    FOREIGN KEY (neighborhood_id) REFERENCES place (neighborhood_id));
    
CREATE TABLE income (
    year_id INT,
    neighborhood_id CHAR(50),
    population INT,
    RFD NUMERIC(10,2),
    FOREIGN KEY (year_id) REFERENCES years (year_id),
    FOREIGN KEY (neighborhood_id) REFERENCES place (neighborhood_id));