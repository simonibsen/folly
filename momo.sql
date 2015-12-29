CREATE DATABASE  IF NOT EXISTS `momo_dev` /*!40100 DEFAULT CHARACTER SET latin1 */;
USE `momo_dev`;
-- MySQL dump 10.13  Distrib 5.6.13, for osx10.6 (i386)
--
-- Host: localhost    Database: momo_dev
-- ------------------------------------------------------
-- Server version	5.5.10

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `sector`
--

DROP TABLE IF EXISTS `sector`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sector` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=big5;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stock_history`
--

DROP TABLE IF EXISTS `stock_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `stock_history` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `data_id` int(11) DEFAULT NULL,
  `date` int(11) DEFAULT NULL,
  `open` decimal(5,2) DEFAULT NULL,
  `high` decimal(5,2) DEFAULT NULL,
  `low` decimal(5,2) DEFAULT NULL,
  `close` decimal(5,2) DEFAULT NULL,
  `percentage_change_price` decimal(5,2) DEFAULT NULL,
  `volume` int(11) DEFAULT NULL,
  `percentage_change_volume` decimal(5,2) DEFAULT NULL,
  `previous_close` varchar(45) DEFAULT NULL,
  `average_daily_volume` varchar(45) DEFAULT NULL,
  `52_week_range` varchar(45) DEFAULT NULL,
  `pct_change_from_52_wk_low` varchar(5) DEFAULT NULL,
  `pct_change_from_52_wk_hi` varchar(5) DEFAULT NULL,
  `50_ma` varchar(5) DEFAULT NULL,
  `pct_change_from_50_ma` varchar(5) DEFAULT NULL,
  `200_ma` varchar(5) DEFAULT NULL,
  `pct_change_from_200_ma` varchar(5) DEFAULT NULL,
  `earnings_to_share` varchar(10) DEFAULT NULL,
  `p2e_ratio` varchar(5) DEFAULT NULL,
  `short_ratio` varchar(5) DEFAULT NULL,
  `dividend_pay_date` varchar(45) DEFAULT NULL,
  `ex_dividend_pay_date` varchar(45) DEFAULT NULL,
  `dividend_yield` varchar(45) DEFAULT NULL,
  `float_shares` varchar(45) DEFAULT NULL,
  `market_cap` varchar(45) DEFAULT NULL,
  `1yr_target_price` varchar(45) DEFAULT NULL,
  `PEG` varchar(45) DEFAULT NULL,
  `book_value` varchar(45) DEFAULT NULL,
  `stock_exchange` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `date_index_asc` (`date`),
  KEY `stock_id` (`data_id`),
  KEY `date_data_index` (`data_id`,`date`)
) ENGINE=InnoDB AUTO_INCREMENT=28554182 DEFAULT CHARSET=big5;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `calc_store_descriptor`
--

DROP TABLE IF EXISTS `calc_store_descriptor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `calc_store_descriptor` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `calc_store_name` varchar(45) DEFAULT NULL,
  `fx_name` varchar(45) DEFAULT NULL,
  `fx_arg` varchar(45) DEFAULT NULL,
  `source_t` varchar(45) DEFAULT NULL,
  `source_c` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=65 DEFAULT CHARSET=big5;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `industry_history`
--

DROP TABLE IF EXISTS `industry_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `industry_history` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `data_id` int(11) DEFAULT NULL,
  `date` int(11) DEFAULT NULL,
  `price_percentage_unweighted` decimal(5,2) DEFAULT NULL,
  `volume` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `data_id_date` (`data_id`,`date`)
) ENGINE=InnoDB AUTO_INCREMENT=397313 DEFAULT CHARSET=big5;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `market_dates`
--

DROP TABLE IF EXISTS `market_dates`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `market_dates` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `date` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `date_UNIQUE` (`date`)
) ENGINE=InnoDB AUTO_INCREMENT=1780 DEFAULT CHARSET=big5;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `calc_store`
--

DROP TABLE IF EXISTS `calc_store`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `calc_store` (
  `id` int(11) NOT NULL DEFAULT '0',
  `calc_store_descriptor_id` int(11) DEFAULT NULL,
  `stock_id` int(11) DEFAULT NULL,
  `date` int(11) DEFAULT NULL,
  `value` decimal(10,2) DEFAULT NULL,
  UNIQUE KEY `calc_store_descriptor_id` (`calc_store_descriptor_id`,`stock_id`,`date`),
  KEY `stock_id_index` (`stock_id`),
  KEY `cs_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=big5;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `industry`
--

DROP TABLE IF EXISTS `industry`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `industry` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(45) DEFAULT NULL,
  `sector_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name_UNIQUE` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=217 DEFAULT CHARSET=big5;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `rating_pool`
--

DROP TABLE IF EXISTS `rating_pool`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `rating_pool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stock_id` int(11) DEFAULT NULL,
  `date` int(11) DEFAULT NULL,
  `value` decimal(10,2) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8132043 DEFAULT CHARSET=big5;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stock`
--

DROP TABLE IF EXISTS `stock`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `stock` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `symbol` varchar(45) DEFAULT NULL,
  `industry_id` varchar(45) DEFAULT NULL,
  `status` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=18585 DEFAULT CHARSET=big5;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2015-12-29  9:23:13
