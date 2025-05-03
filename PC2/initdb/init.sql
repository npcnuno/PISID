-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Host: localhost
-- Generation Time: Apr 30, 2025 at 05:03 PM
-- Server version: 10.4.32-MariaDB
-- PHP Version: 8.2.12

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `mydb`
--

-- --------------------------------------------------------

--
-- Table structure for table `Jogo`
--

CREATE TABLE `Jogo` (
                        `idJogo` int(11) NOT NULL,
                        `email` varchar(50) DEFAULT NULL,
                        `descricao` text DEFAULT NULL,
                        `jogador` varchar(50) DEFAULT NULL,
                        `scoreTotal` int(11) DEFAULT NULL,
                        `dataHoraInicio` varchar(45) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Table structure for table `MedicaoPassagem`
--

CREATE TABLE `MedicaoPassagem` (
                                   `idMedicao` int(11) NOT NULL,
                                   `hora` timestamp NULL DEFAULT NULL,
                                   `salaOrigem` int(11) DEFAULT NULL,
                                   `salaDestino` int(11) DEFAULT NULL,
                                   `marsami` int(11) DEFAULT NULL,
                                   `status` int(11) DEFAULT NULL,
                                   `idJogo` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;


-- --------------------------------------------------------

--
-- Table structure for table `Mensagens`
--

CREATE TABLE `Mensagens` (
                             `idMensagem` int(11) NOT NULL,
                             `hora` timestamp NULL DEFAULT NULL,
                             `sensor` int(11) DEFAULT NULL,
                             `leitura` double DEFAULT NULL,
                             `tipoAlerta` varchar(50) DEFAULT NULL,
                             `mensagem` varchar(100) DEFAULT NULL,
                             `horaEscrita` timestamp NULL DEFAULT NULL,
                             `idJogo` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Table structure for table `OcupacaoLabirinto`
--

CREATE TABLE `OcupacaoLabirinto` (
                                     `sala` int(11) NOT NULL,
                                     `numeroMarsamiOdd` int(11) NOT NULL DEFAULT 0,
                                     `numeroMarsamiEven` int(11) NOT NULL DEFAULT 0,
                                     `score` int(11) DEFAULT NULL,
                                     `idJogo` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;


-- --------------------------------------------------------

--
-- Table structure for table `Sound`
--

CREATE TABLE `Sound` (
                         `idSound` int(11) NOT NULL,
                         `hora` timestamp NULL DEFAULT NULL,
                         `sound` decimal(10,2) DEFAULT NULL,
                         `idJogo` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Table structure for table `User`
--

CREATE TABLE `User` (
                        `email` varchar(50) NOT NULL,
                        `nome` varchar(100) DEFAULT NULL,
                        `telemovel` varchar(12) DEFAULT NULL,
                        `tipo` varchar(3) DEFAULT NULL,
                        `grupo` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `Jogo`
--
ALTER TABLE `Jogo`
    ADD PRIMARY KEY (`idJogo`),
  ADD KEY `fk_Jogo_email_idx` (`email`);

--
-- Indexes for table `MedicaoPassagem`
--
ALTER TABLE `MedicaoPassagem`
    ADD PRIMARY KEY (`idMedicao`),
  ADD KEY `fk_Medicao_Jogo_idx` (`idJogo`);

--
-- Indexes for table `Mensagens`
--
ALTER TABLE `Mensagens`
    ADD PRIMARY KEY (`idMensagem`),
  ADD KEY `fk_Alerta_Jogo_idx` (`idJogo`);

--
-- Indexes for table `OcupacaoLabirinto`
--
ALTER TABLE `OcupacaoLabirinto`
    ADD PRIMARY KEY (`sala`),
  ADD KEY `fk_Ocupacao_Jogo_idx` (`idJogo`);

--
-- Indexes for table `Sound`
--
ALTER TABLE `Sound`
    ADD PRIMARY KEY (`idSound`),
  ADD KEY `fk_Sound_Jogo_idx` (`idJogo`);

--
-- Indexes for table `User`
--
ALTER TABLE `User`
    ADD PRIMARY KEY (`email`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `Jogo`
--
ALTER TABLE `Jogo`
    MODIFY `idJogo` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- AUTO_INCREMENT for table `MedicaoPassagem`
--
ALTER TABLE `MedicaoPassagem`
    MODIFY `idMedicao` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=3;

--
-- AUTO_INCREMENT for table `Mensagens`
--
ALTER TABLE `Mensagens`
    MODIFY `idMensagem` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `Sound`
--
ALTER TABLE `Sound`
    MODIFY `idSound` int(11) NOT NULL AUTO_INCREMENT;

--
-- Constraints for dumped tables
--

--
-- Constraints for table `Jogo`
--
ALTER TABLE `Jogo`
    ADD CONSTRAINT `fk_Jogo_email` FOREIGN KEY (`email`) REFERENCES `User` (`email`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Constraints for table `MedicaoPassagem`
--
ALTER TABLE `MedicaoPassagem`
    ADD CONSTRAINT `fk_Medicao_Jogo` FOREIGN KEY (`idJogo`) REFERENCES `Jogo` (`idJogo`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Constraints for table `Mensagens`
--
ALTER TABLE `Mensagens`
    ADD CONSTRAINT `fk_Alerta_Jogo` FOREIGN KEY (`idJogo`) REFERENCES `Jogo` (`idJogo`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Constraints for table `OcupacaoLabirinto`
--
ALTER TABLE `OcupacaoLabirinto`
    ADD CONSTRAINT `fk_Ocupacao_Jogo` FOREIGN KEY (`idJogo`) REFERENCES `Jogo` (`idJogo`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Constraints for table `Sound`
--
ALTER TABLE `Sound`
    ADD CONSTRAINT `fk_Sound_Jogo` FOREIGN KEY (`idJogo`) REFERENCES `Jogo` (`idJogo`) ON DELETE NO ACTION ON UPDATE NO ACTION;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;


DELIMITER $$

CREATE TRIGGER `atualiza_scoreTotal_jogo`
AFTER INSERT ON `OcupacaoLabirinto`
FOR EACH ROW
BEGIN
    UPDATE Jogo
    SET scoreTotal = (
        SELECT IFNULL(SUM(score), 0)
        FROM OcupacaoLabirinto
        WHERE idJogo = NEW.idJogo
    )
    WHERE idJogo = NEW.idJogo;
END$$

DELIMITER ;


DELIMITER $$

CREATE TRIGGER `update_ocupacao_labirinto`
AFTER INSERT ON `MedicaoPassagem`
FOR EACH ROW
BEGIN
    DECLARE odd BOOLEAN;
    SET odd = MOD(NEW.marsami, 2) = 1;

    -- Ensure both origin and destination exist
    INSERT IGNORE INTO OcupacaoLabirinto (sala, idJogo, numeroMarsamiOdd, numeroMarsamiEven, score)
    VALUES
        (NEW.salaOrigem, NEW.idJogo, 0, 0, 0),
        (NEW.salaDestino, NEW.idJogo, 0, 0, 0);

    -- Perform the appropriate update
    IF odd THEN
        UPDATE OcupacaoLabirinto
        SET numeroMarsamiOdd = numeroMarsamiOdd - 1
        WHERE sala = NEW.salaOrigem AND idJogo = NEW.idJogo;

        UPDATE OcupacaoLabirinto
        SET numeroMarsamiOdd = numeroMarsamiOdd + 1
        WHERE sala = NEW.salaDestino AND idJogo = NEW.idJogo;
    ELSE
        UPDATE OcupacaoLabirinto
        SET numeroMarsamiEven = numeroMarsamiEven - 1
        WHERE sala = NEW.salaOrigem AND idJogo = NEW.idJogo;

        UPDATE OcupacaoLabirinto
        SET numeroMarsamiEven = numeroMarsamiEven + 1
        WHERE sala = NEW.salaDestino AND idJogo = NEW.idJogo;
    END IF;
END$$

DELIMITER ;



DELIMITER $$

CREATE TRIGGER `check_sound_threshold`
AFTER INSERT ON `Sound`
FOR EACH ROW
BEGIN
    IF NEW.sound > 21 THEN
        INSERT INTO Mensagens (hora, sensor, leitura, tipoAlerta, mensagem, horaEscrita, idJogo)
        VALUES (NOW(), NULL, NEW.sound, 'SOM', 'sound bigger than 21', NOW(), NEW.idJogo);
    END IF;
END$$

DELIMITER ;


