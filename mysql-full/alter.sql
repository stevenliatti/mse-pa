ALTER TABLE Booking ADD UNIQUE(flight_id, seat_number);
ALTER TABLE BookingSeatNumber ADD UNIQUE(booking_flight_id, timestamp, seat_number);
