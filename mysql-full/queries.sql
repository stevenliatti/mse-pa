SELECT flight_id, COUNT(flight_id), seats FROM Booking JOIN Flight ON Flight.id = flight_id JOIN Aircraft ON Flight.aircraft_id = Aircraft.id GROUP BY flight_id

SELECT flight_id, COUNT(flight_id) / seats AS med FROM Booking JOIN Flight ON Flight.id = flight_id JOIN Aircraft ON Flight.aircraft_id = Aircraft.id GROUP BY flight_id ORDER BY `med`  DESC LIMIT 10

SELECT * FROM Client AS C JOIN ClientFirstname ON C.id = ClientFirstname.client_id JOIN ClientLastname ON C.id = ClientLastname.client_id JOIN ClientEmail ON C.id = ClientEmail.client_id WHERE id = 1

SELECT id, firstname, lastname, email, ClientFirstname.timestamp, ClientLastname.timestamp, ClientEmail.timestamp FROM Client AS C JOIN ClientFirstname ON C.id = ClientFirstname.client_id JOIN ClientLastname ON C.id = ClientLastname.client_id JOIN ClientEmail ON C.id = ClientEmail.client_id WHERE id = 1

SELECT
id,
(SELECT firstname FROM ClientFirstname WHERE client_id = 1 ORDER BY timestamp DESC LIMIT 1) AS firstname,
(SELECT lastname FROM ClientLastname WHERE client_id = 1 ORDER BY timestamp DESC LIMIT 1) AS lastname,
(SELECT email FROM ClientEmail WHERE client_id = 1 ORDER BY timestamp DESC LIMIT 1) AS email
FROM Client WHERE id = 1

-- Get all full clients with each state of history per client
SELECT id, firstname, lastname, email, ClientFirstname.timestamp AS tsF, ClientLastname.timestamp AS tsL, ClientEmail.timestamp AS tsE
FROM Client AS C 
JOIN ClientFirstname ON C.id = ClientFirstname.client_id
JOIN ClientLastname ON C.id = ClientLastname.client_id
JOIN ClientEmail ON C.id = ClientEmail.client_id
ORDER BY id, tsF DESC, tsL DESC, tsE DESC

-- Get all full clients with only last state of history per client
SELECT id, firstname, lastname, email, tsF, tsL, tsE FROM (
    SELECT ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY 
        id,
        ClientFirstname.timestamp DESC,
        ClientLastname.timestamp DESC,
        ClientEmail.timestamp DESC
    ) AS virtual_id, 
    id, firstname, lastname, email,
    ClientFirstname.timestamp AS tsF,
    ClientLastname.timestamp AS tsL,
    ClientEmail.timestamp AS tsE
    FROM Client AS C 
    JOIN ClientFirstname ON C.id = ClientFirstname.client_id
    JOIN ClientLastname ON C.id = ClientLastname.client_id
    JOIN ClientEmail ON C.id = ClientEmail.client_id
) AS sorted_timestamps
GROUP BY sorted_timestamps.id
ORDER BY sorted_timestamps.id

-- Get all full flights with only last state of history per flight
SELECT id, datetime, departure_iata_code, arrival_iata_code, aircraft_id FROM (
    SELECT ROW_NUMBER() OVER (
        PARTITION BY flight_id
        ORDER BY flight_id, timestamp DESC
    ) AS virtual_id, flight_id, datetime
    FROM FlightDatetime
) AS sorted_timestamps
JOIN Flight ON Flight.id = sorted_timestamps.flight_id
GROUP BY sorted_timestamps.flight_id
ORDER BY sorted_timestamps.flight_id

-- Get flights per aircraft brand
SELECT brand, COUNT(Flight.id) FROM Flight JOIN Aircraft ON Flight.aircraft_id = Aircraft.id GROUP BY brand

-- Get clients email (last state)
SELECT email FROM (
    SELECT ROW_NUMBER() OVER (
        PARTITION BY client_id
        ORDER BY client_id, timestamp DESC
    ) AS virtual_id, client_id, email
    FROM ClientEmail
) AS sorted_timestamps
GROUP BY sorted_timestamps.client_id
ORDER BY sorted_timestamps.client_id

-- Get last state for each bookings
SELECT booking_flight_id, booking_client_id, seat_number FROM (
    SELECT ROW_NUMBER() OVER (
        PARTITION BY booking_flight_id, booking_client_id
        ORDER BY seat_number, timestamp DESC
    ) AS virtual_id, booking_flight_id, booking_client_id, seat_number
    FROM BookingSeatNumber
) AS sorted_timestamps
GROUP BY booking_flight_id, booking_client_id
ORDER BY booking_flight_id







SELECT COUNT(*) / seats FROM (
SELECT seat_number, seats FROM (
SELECT ROW_NUMBER() OVER (
PARTITION BY booking_flight_id, booking_client_id
ORDER BY seat_number, timestamp DESC
) AS virtual_id, booking_flight_id, booking_client_id, seat_number
FROM BookingSeatNumber
) AS sorted_timestamps
JOIN Flight ON Flight.id = booking_flight_id
JOIN Aircraft ON Aircraft.id = Flight.aircraft_id
WHERE booking_flight_id = 1
GROUP BY booking_flight_id, booking_client_id
ORDER BY booking_flight_id
) AS bob