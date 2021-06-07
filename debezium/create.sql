-- MySQL Script generated by MySQL Workbench
-- Tue 11 May 2021 06:41:50 PM CEST
-- Model: New Model    Version: 1.0
-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema fly
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema fly
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `fly` ;
USE `fly` ;

-- -----------------------------------------------------
-- Table `fly`.`Client`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `fly`.`Client` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `firstname` VARCHAR(45) NOT NULL,
  `lastname` VARCHAR(45) NOT NULL,
  `email` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `email_UNIQUE` (`email` ASC) VISIBLE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `fly`.`Airport`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `fly`.`Airport` (
  `iata_code` CHAR(3) NOT NULL,
  `name` VARCHAR(100) NOT NULL,
  PRIMARY KEY (`iata_code`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `fly`.`Aircraft`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `fly`.`Aircraft` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `brand` VARCHAR(45) NOT NULL,
  `seats` INT NOT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `fly`.`Flight`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `fly`.`Flight` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `datetime` DATETIME NOT NULL,
  `departure_iata_code` CHAR(3) NOT NULL,
  `arrival_iata_code` CHAR(3) NOT NULL,
  `aircraft_id` INT NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_Flight_Airport1_idx` (`departure_iata_code` ASC) VISIBLE,
  INDEX `fk_Flight_Airport2_idx` (`arrival_iata_code` ASC) VISIBLE,
  INDEX `fk_Flight_Aircraft1_idx` (`aircraft_id` ASC) VISIBLE,
  CONSTRAINT `fk_Flight_Airport1`
    FOREIGN KEY (`departure_iata_code`)
    REFERENCES `fly`.`Airport` (`iata_code`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Flight_Airport2`
    FOREIGN KEY (`arrival_iata_code`)
    REFERENCES `fly`.`Airport` (`iata_code`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Flight_Aircraft1`
    FOREIGN KEY (`aircraft_id`)
    REFERENCES `fly`.`Aircraft` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `fly`.`Booking`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `fly`.`Booking` (
  `flight_id` INT NOT NULL,
  `client_id` INT NOT NULL,
  `seat_number` INT NOT NULL,
  `state` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`flight_id`, `client_id`),
  INDEX `fk_Flight_has_Client_Client_idx` (`client_id` ASC) VISIBLE,
  INDEX `fk_Flight_has_Client_Flight_idx` (`flight_id` ASC) VISIBLE,
  CONSTRAINT `fk_Flight_has_Client_Flight`
    FOREIGN KEY (`flight_id`)
    REFERENCES `fly`.`Flight` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Flight_has_Client_Client1`
    FOREIGN KEY (`client_id`)
    REFERENCES `fly`.`Client` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
PACK_KEYS = DEFAULT;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;