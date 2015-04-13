CREATE TABLE teams (
	id serial PRIMARY KEY,
        name varchar
);

CREATE TABLE matches (
	id serial PRIMARY KEY,
        home_team serial references teams(id),
        away_team serial references teams(id),
        game_date date
);

CREATE TABLE players (
	id serial PRIMARY KEY,
        name varchar
);

CREATE TABLE shots (
	id serial PRIMARY KEY,
        match serial references matches(id),
        team serial references teams(id),
	player serial references players(id),
	hit boolean,
	x smallserial,
        y smallserial
);
