
create table aircrafts (
  aircraft_code char(3) not null,
  model text not null,
  range integer not null,
  primary key(aircraft_code)
);

create table airports (
  airport_code char(3) not null,
  airport_name text not null,
  city text not null,
  coordinates text not null,
  timezone text not null,
  primary key(airport_code)
);

create table bookings (
  book_ref char(6) not null,
  book_date TIMESTAMP WITH TIME ZONE not null,
  total_amount numeric(10,2) not null,
  primary key(book_ref)
);

create table flights (
  flight_id serial not null,
  flight_no char(6) not null,
  scheduled_departure TIMESTAMP WITH TIME ZONE not null,
  scheduled_arrival TIMESTAMP WITH TIME ZONE not null,
  departure_airport char(3) not null,
  arrival_airport char(3) not null,
  status varchar(20) not null,
  aircraft_code char(3) not null,
  actual_departure TIMESTAMP WITH TIME ZONE,
  actual_arrival TIMESTAMP WITH TIME ZONE,
  primary key(flight_id)
);

--
create table seats (
  aircraft_code char(3) not null,
  seat_no varchar(4) not null,
  fare_conditions varchar(10) not null,
  primary key(aircraft_code, seat_no)
);

create table tickets (
  ticket_no char(13) not null,
  book_ref char(6) not null,
  passenger_id varchar(20) not null,
  passenger_name text not null,
  contact_data text,
  primary key(ticket_no)
);


create table ticket_flights (
  ticket_no char(13) not null,
  flight_id integer not null,
  fare_conditions varchar(10) not null,
  amount numeric(10, 2) not null,
  primary key(ticket_no, flight_id)
);

create table boarding_passes (
  ticket_no char(13) not null,
  flight_id integer not null,
  boarding_no integer not null,
  seat_no varchar(4) not null,
  primary key(ticket_no, flight_id)
);