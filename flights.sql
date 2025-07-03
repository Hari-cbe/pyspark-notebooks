DROP TABLE IF EXISTS Journey

CREATE TABLE Journey
(
 FlightID INT,
 StartPoint varchar(max),
 [EndPoint] varchar(max)
)

INSERT INTO Journey
VALUES
(1,'Hyderabad','Chennai'),
(1,'Chennai','Mumbai'),
(1,'Mumbai','Kolkata'),
(2,'Raipur','Delhi'),
(2,'Delhi','Mumbai')

Select * FROM Journey;