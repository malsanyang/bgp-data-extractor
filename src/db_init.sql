USE ejaw_data;

CREATE TABLE route_records (
    id INT AUTO_INCREMENT PRIMARY KEY,
    year VARCHAR(4) NOT NULL,
    month VARCHAR(2) NOT NULL,
    day VARCHAR(2) NOT NULL,
    hour VARCHAR(2) NOT NULL,
    time INT NOT NULL,
    prefix VARCHAR(50) NOT NULL,
    origin VARCHAR(255) NOT NULL,
    peer_cnt INT NOT NULL
);