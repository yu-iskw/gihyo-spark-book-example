--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

CREATE DATABASE IF NOT EXISTS gihyo_spark;

USE gihyo_spark;
DROP TABLE IF EXISTS `person`;
CREATE TABLE IF NOT EXISTS `president` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(64) NOT NULL,
  PRIMARY KEY (`id`)
);

INSERT INTO `gihyo_spark`.`president` (name) VALUES ("George Washington");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("John Adams");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Thomas Jefferson");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("James Madison");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("James Monroe");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("John Quincy Adams");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Andrew Jackson");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Martin Van Buren");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("William Henry Harrison");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("John Tyler");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("James K. Polk");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Zachary Taylor");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Millard Fillmore");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Franklin Pierce");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("James Buchanan");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Abraham Lincoln");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Andrew Johnson");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Ulysses S. Grant");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Rutherford B. Hayes");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("James A. Garfield");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Chester Arthur");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Grover Cleveland");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Benjamin Harrison");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Grover Cleveland");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("William McKinley");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Theodore Roosevelt");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("William Howard Taft");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Woodrow Wilson");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Warren G. Harding");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Calvin Coolidge");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Herbert Hoover");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Franklin D. Roosevelt");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Harry S Truman");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Dwight D. Eisenhower");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("John F. Kennedy");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Lyndon B. Johnson");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Richard Nixon");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Gerald Ford");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Jimmy Carter");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Ronald Reagan");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("George Bush");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Bill Clinton");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("George W. Bush");
INSERT INTO `gihyo_spark`.`president` (name) VALUES ("Barack Obama");
