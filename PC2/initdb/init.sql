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

CREATE ROLE IF NOT EXISTS 'admin';	# administrador
CREATE ROLE IF NOT EXISTS 'player';	# jogador
CREATE ROLE IF NOT EXISTS 'tester';	# tester
-- --------------------------------------------------------

--
-- Table structure for table `Jogo`
--

-- Table structure for table `Jogo`
CREATE TABLE `Jogo` (
  `idJogo` int(11) NOT NULL,
  `email` varchar(50) DEFAULT NULL,
  `descricao` text DEFAULT NULL,
  `jogador` varchar(50) DEFAULT NULL,
  `scoreTotal` int(11) DEFAULT NULL,
  `dataHoraInicio` timestamp NULL DEFAULT NULL,
  `estado` boolean DEFAULT FALSE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- ... and similarly for other tables ...

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `Sound`
--

CREATE TABLE `Sound` (
  `idSound` int(11) NOT NULL,
  `hora` timestamp NULL DEFAULT NULL,
  `sound` decimal(10,2) DEFAULT NULL,
  `idJogo` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `User`
--

CREATE TABLE `Users` (
  `email` varchar(50) NOT NULL,
  `nome` varchar(100) DEFAULT NULL,
  `telemovel` varchar(12) DEFAULT NULL,
  `tipo` SET('admin','player','tester') DEFAULT NULL,
  `grupo` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



--
-- Indexes for dumped tables
--

ALTER TABLE `Jogo`
  ADD PRIMARY KEY (`idJogo`),
  ADD KEY `fk_Jogo_email_idx` (`email`);

ALTER TABLE `MedicaoPassagem`
  ADD PRIMARY KEY (`idMedicao`),
  ADD KEY `fk_Medicao_Jogo_idx` (`idJogo`);

ALTER TABLE `Mensagens`
  ADD PRIMARY KEY (`idMensagem`),
  ADD KEY `fk_Alerta_Jogo_idx` (`idJogo`);

ALTER TABLE `OcupacaoLabirinto`
  ADD PRIMARY KEY (`sala`),
  ADD KEY `fk_Ocupacao_Jogo_idx` (`idJogo`);

ALTER TABLE `Sound`
  ADD PRIMARY KEY (`idSound`),
  ADD KEY `fk_Sound_Jogo_idx` (`idJogo`);

ALTER TABLE `Users`
  ADD PRIMARY KEY (`email`);

--
-- AUTO_INCREMENT for tables
--

ALTER TABLE `Jogo`
  MODIFY `idJogo` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1;

ALTER TABLE `MedicaoPassagem`
  MODIFY `idMedicao` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1;

ALTER TABLE `Mensagens`
  MODIFY `idMensagem` int(11) NOT NULL AUTO_INCREMENT;

ALTER TABLE `Sound`
  MODIFY `idSound` int(11) NOT NULL AUTO_INCREMENT;

--
-- Constraints
--

ALTER TABLE `Jogo`
  ADD CONSTRAINT `fk_Jogo_email` FOREIGN KEY (`email`) REFERENCES `Users` (`email`) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE `MedicaoPassagem`
  ADD CONSTRAINT `fk_Medicao_Jogo` FOREIGN KEY (`idJogo`) REFERENCES `Jogo` (`idJogo`) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE `Mensagens`
  ADD CONSTRAINT `fk_Alerta_Jogo` FOREIGN KEY (`idJogo`) REFERENCES `Jogo` (`idJogo`) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE `OcupacaoLabirinto`
  ADD CONSTRAINT `fk_Ocupacao_Jogo` FOREIGN KEY (`idJogo`) REFERENCES `Jogo` (`idJogo`) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE `Sound`
  ADD CONSTRAINT `fk_Sound_Jogo` FOREIGN KEY (`idJogo`) REFERENCES `Jogo` (`idJogo`) ON DELETE NO ACTION ON UPDATE NO ACTION;

COMMIT;


######################_TEST_DATA_######################


-- Fill Users
INSERT INTO Users (email, nome, telemovel, tipo, grupo) VALUES
('alice@example.com', 'Alice Silva', '912345678', 'admin', 1),
('bob@example.com', 'Bob Costa', '913456789', 'player', 2),
('carla@example.com', 'Carla Dias', '914567890', 'tester', 3);

-- Fill Jogo
INSERT INTO Jogo (email, descricao, jogador, scoreTotal, dataHoraInicio) VALUES
('bob@example.com', 'Jogo de teste do Bob', 'Bob Costa', 0, '2025-04-30 14:00:00'),
('carla@example.com', 'Teste do labirinto', 'Carla Dias', 0, '2025-04-30 15:30:00');

-- Fill MedicaoPassagem
INSERT INTO MedicaoPassagem (hora, salaOrigem, salaDestino, marsami, status, idJogo) VALUES
('2025-04-30 14:10:00', 1, 2, 101, 1, 1),
('2025-04-30 14:12:00', 2, 3, 102, 1, 1),
('2025-04-30 15:40:00', 1, 3, 201, 1, 2);

-- Fill OcupacaoLabirinto (will be auto-filled by trigger, but we can also manually test it)
INSERT INTO OcupacaoLabirinto (sala, numeroMarsamiOdd, numeroMarsamiEven, score, idJogo) VALUES
(1, 1, 0, 5, 1),
(2, 0, 1, 10, 1),
(3, 0, 0, 3, 2);

-- Fill Sound
INSERT INTO Sound (hora, sound, idJogo) VALUES
('2025-04-30 14:15:00', 19.50, 1),
('2025-04-30 14:20:00', 23.10, 1),
('2025-04-30 15:45:00', 22.30, 2);

-- Fill Mensagens (should be filled automatically by trigger when sound > 21, but we can add test data too)
INSERT INTO Mensagens (hora, sensor, leitura, tipoAlerta, mensagem, horaEscrita, idJogo) VALUES
('2025-04-30 14:21:00', NULL, 23.10, 'SOM', 'sound bigger than 21', '2025-04-30 14:21:00', 1),
('2025-04-30 15:46:00', NULL, 22.30, 'SOM', 'sound bigger than 21', '2025-04-30 15:46:00', 2);


-- Triggers

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

  INSERT IGNORE INTO OcupacaoLabirinto (sala, idJogo, numeroMarsamiOdd, numeroMarsamiEven, score)
  VALUES
    (NEW.salaOrigem, NEW.idJogo, 0, 0, 0),
    (NEW.salaDestino, NEW.idJogo, 0, 0, 0);

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

CREATE DEFINER='root'@'%' PROCEDURE Criar_utilizador(
    IN p_email VARCHAR(50),
    IN p_nome VARCHAR(100),
    IN p_telemovel VARCHAR(12),
    IN p_tipo SET('admin','player','tester'),
    IN p_grupo INT,
    IN p_pass VARCHAR(100)
)
BEGIN
    DECLARE v_username VARCHAR(40);
    DECLARE at_pos INT;

    -- Extrai o username do email
    SET at_pos = LOCATE('@', p_email);
    SET v_username = LEFT(p_email, at_pos - 1);

    IF at_pos <= 1 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: Email inválido';
    END IF;

    -- Verifica se o email já existe
    IF EXISTS (SELECT 1 FROM Users WHERE email = p_email) THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: Email já está registado no sistema';
    ELSE
        -- Insere dados na tabela Users (SEM armazenar senha)
        INSERT INTO Users (email, nome, telemovel, tipo, grupo)
        VALUES (p_email, p_nome, p_telemovel, p_tipo, p_grupo);

        -- Cria o utilizador MySQL
        SET @sql_create_user = CONCAT('CREATE USER \'', v_username, '\'@\'%\' IDENTIFIED BY \'', p_pass, '\'');
        PREPARE stmt FROM @sql_create_user;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- Concede role
        SET @sql_grant = CONCAT('GRANT ', p_tipo, ' TO \'', v_username, '\'@\'%\'');
        PREPARE grant_stmt FROM @sql_grant;
        EXECUTE grant_stmt;
        DEALLOCATE PREPARE grant_stmt;

        -- Define a role padrão
        SET @sql_default_role = CONCAT('SET DEFAULT ROLE ', p_tipo, ' TO \'', v_username, '\'@\'%\'');
        PREPARE role_stmt FROM @sql_default_role;
        EXECUTE role_stmt;
        DEALLOCATE PREPARE role_stmt;

        -- Concede privilégios em todos os schemas
        SET @sql_all_schema_privileges = CONCAT('GRANT ALL PRIVILEGES ON *.* TO \'', v_username, '\'@\'%\' WITH GRANT OPTION');
        PREPARE all_schema_stmt FROM @sql_all_schema_privileges;
        EXECUTE all_schema_stmt;
        DEALLOCATE PREPARE all_schema_stmt;

        -- Atualiza privilégios
        FLUSH PRIVILEGES;
    END IF;
END $$

DELIMITER ;


DELIMITER $$

CREATE DEFINER='root'@'%' PROCEDURE `Remover_utilizador`(
	IN p_email VARCHAR(50)
    )
BEGIN
    DECLARE v_username VARCHAR(40);
	DECLARE at_pos INT;

    -- Extrai o username do email
	SET at_pos = LOCATE('@', p_email);
	SET v_username = LEFT(p_email, at_pos - 1);

    -- Verifica se o email existe na tabela Users
    IF NOT EXISTS (SELECT 1 FROM Users WHERE email = p_email) THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: Email não encontrado no sistema';
    ELSE
         -- Verifica se o utilizador MySQL existe
        SET @sql_check_user = CONCAT('SELECT COUNT(*) INTO @user_count FROM mysql.user WHERE User = \'', v_username, '\' AND Host = \'%\'');
        PREPARE stmt_check FROM @sql_check_user;
        EXECUTE stmt_check;
        DEALLOCATE PREPARE stmt_check;

        -- Se o utilizador MySQL existir, remove-o
        IF @user_count > 0 THEN
            -- Revoga todos os privilégios primeiro
            SET @sql_revoke = CONCAT('REVOKE ALL PRIVILEGES, GRANT OPTION FROM \'', v_username, '\'@\'%\'');
            PREPARE stmt_revoke FROM @sql_revoke;
            EXECUTE stmt_revoke;
            DEALLOCATE PREPARE stmt_revoke;

            -- Remove o utilizador
            SET @sql_drop_user = CONCAT('DROP USER \'', v_username, '\'@\'%\'');
            PREPARE stmt_drop FROM @sql_drop_user;
            EXECUTE stmt_drop;
            DEALLOCATE PREPARE stmt_drop;

            -- Atualiza privilégios
            FLUSH PRIVILEGES;

        END IF;

		-- Remove da tabela Users
        DELETE FROM Users WHERE email = p_email;

    END IF;
END$$

DELIMITER ;


DELIMITER $$

CREATE DEFINER='root'@'%' PROCEDURE Alterar_jogo(
	IN p_idJogo INT,
    IN p_descricao TEXT,
    IN p_jogador VARCHAR(100)
    -- IN p_scoreTotal INT,
    -- IN p_dataHorainicio DATETIME
)
BEGIN
	DECLARE v_requestEmail VARCHAR(50);
    DECLARE v_userType VARCHAR(20);
    DECLARE v_gameIsRunning BOOLEAN; -- 0 (isRunnig) 1 (jogo criado e ainda n começado e gameEnded)
    DECLARE v_emailJogo VARCHAR(50);

	-- Metodo para obter o email do usuário atual
    SET v_requestEmail = CURRENT_USER();

    -- Verifica se o jogo existe e obtém o proprietário do email do dono
    SELECT email INTO v_emailJogo FROM Jogo WHERE idJogo = p_idJogo;



    IF v_emailJogo IS NULL THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Error: Game does not exist.';
    ELSE
        -- Verifica se já se pode mexer na bd (jogo not runnig)
        SELECT estado INTO v_gameIsRunning FROM Jogo WHERE idJogo = p_idJogo;
        -- Obtém o tipo de usuário
        SELECT tipo INTO v_userType FROM Users WHERE email = v_requestEmail;

        -- Verifica as permissões
        IF v_userType = 'admin' OR (v_userType = 'tester' AND v_requestEmail = v_emailJogo) THEN
            IF !v_gameIsRunning THEN
				-- Atualiza o jogo
                UPDATE Jogo
                SET
                    descricao = IFNULL(p_descricao, descricao),
                    jogador = IFNULL(p_jogador, jogador)
                    -- scoreTotal = IFNULL(p_scoreTotal, scoreTotal),
                    -- dataHoraInicio = IFNULL(p_dataHoraInicio, dataHoraInicio)
                WHERE idJogo = p_idJogo;
            ELSE
				SIGNAL SQLSTATE '45000'
					SET MESSAGE_TEXT = 'Error: You can not change data in table Jogo while the game is runnig.';
            END IF;
        ELSE
			SIGNAL SQLSTATE '45000'
				SET MESSAGE_TEXT = 'Error: You do not have permission to modify this game.';
        END IF;
    END IF;
END$$

DELIMITER ;


DELIMITER $$

CREATE DEFINER='root'@'%' PROCEDURE Criar_jogo(
    IN p_email VARCHAR(50),
    IN p_descricao TEXT,
    IN p_jogador VARCHAR(100),
    IN p_dataHoraInicio DATETIME
)
BEGIN
    DECLARE v_user_type SET('admin','player','tester');

    -- Verifica se o utilizador existe
    IF NOT EXISTS (SELECT 1 FROM Users WHERE email = p_email) THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: Email não encontrado na tabela Users';
    END IF;

    -- Verifica o tipo de utilizador
    SELECT tipo INTO v_user_type FROM Users WHERE email = p_email;

    IF v_user_type NOT IN ('admin', 'tester') THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Erro: Apenas administradores ou testers podem criar jogos';
    END IF;

    -- Insere o novo jogo
    INSERT INTO Jogo (email, descricao, jogador, scoreTotal, dataHoraInicio, estado)
    VALUES (p_email, p_descricao, p_jogador, 0, p_dataHoraInicio, FALSE);
END$$

DELIMITER ;


# - - administrador - -
# TABLES
-- Conceda acesso ao schema inteiro (substitua 'meu_schema' pelo nome correto)
GRANT SELECT, INSERT, UPDATE, DELETE ON mydb.* TO "admin";
GRANT SELECT, INSERT, UPDATE, DELETE ON Jogo TO "admin";
GRANT SELECT, INSERT, UPDATE, DELETE ON MedicaoPassagem TO "admin";
GRANT SELECT, INSERT, UPDATE, DELETE ON OcupacaoLabirinto TO "admin";
GRANT SELECT, INSERT, UPDATE, DELETE ON Sound TO "admin";
GRANT SELECT, INSERT, UPDATE, DELETE ON Users TO "admin";
GRANT SELECT, INSERT, UPDATE, DELETE ON Mensagens TO "admin";
# STORED PROCEDURES
GRANT EXECUTE ON PROCEDURE Criar_utilizador TO "admin";
#GRANT EXECUTE ON PROCEDURE Alterar_utilizador TO "admin";
GRANT EXECUTE ON PROCEDURE Remover_utilizador TO "admin";
GRANT EXECUTE ON PROCEDURE Criar_jogo TO "admin";
GRANT EXECUTE ON PROCEDURE Alterar_jogo TO "admin";
# GRANT EXECUTE ON PROCEDURE Remover_jogo TO "admin";

# - - jogador - -
# TABLES
GRANT SELECT, INSERT, UPDATE ON Jogo TO "player";
GRANT SELECT, INSERT ON MedicaoPassagem TO "player";
GRANT SELECT, INSERT ON Mensagens TO "player";
GRANT SELECT, INSERT, UPDATE ON OcupacaoLabirinto TO "player";
GRANT SELECT, INSERT ON Sound TO "player";
GRANT SELECT, UPDATE ON Users TO "player";
GRANT SELECT, INSERT ON Mensagens TO "admin";
# STORED PROCEDURES
# GRANT EXECUTE ON PROCEDURE startGame TO "player";
# GRANT EXECUTE ON PROCEDURE getPoints TO "player";
# GRANT EXECUTE ON PROCEDURE endGame TO "player";
GRANT EXECUTE ON PROCEDURE Criar_jogo TO "player";
GRANT EXECUTE ON PROCEDURE Alterar_jogo TO "player";

# - - tester - -
# TABLES
GRANT SELECT ON TO "tester";
GRANT SELECT ON MedicaoPassagem TO "tester";
GRANT SELECT ON Mensagens TO "tester";
GRANT SELECT ON OcupacaoLabirinto TO "tester";
GRANT SELECT ON Sound TO "tester";
GRANT SELECT, UPDATE ON Users TO "tester";
# STORED PROCEDURES
# GRANT EXECUTE ON PROCEDURE startGame TO "tester";
# GRANT EXECUTE ON PROCEDURE getPoints TO "tester";
# GRANT EXECUTE ON PROCEDURE Alterar_utilizador TO "tester";
GRANT EXECUTE ON PROCEDURE Criar_jogo TO "tester";
GRANT EXECUTE ON PROCEDURE Alterar_jogo TO "tester";
