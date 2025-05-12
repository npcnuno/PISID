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
  `grupo` int(11) DEFAULT NULL,
  `ativo` BOOLEAN DEFAULT '1'
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
  ADD CONSTRAINT `fk_Jogo_email` FOREIGN KEY (`email`) REFERENCES `Users` (`email`) ON DELETE NO ACTION ON UPDATE CASCADE;

ALTER TABLE `MedicaoPassagem`
  ADD CONSTRAINT `fk_Medicao_Jogo` FOREIGN KEY (`idJogo`) REFERENCES `Jogo` (`idJogo`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `Mensagens`
  ADD CONSTRAINT `fk_Alerta_Jogo` FOREIGN KEY (`idJogo`) REFERENCES `Jogo` (`idJogo`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `OcupacaoLabirinto`
  ADD CONSTRAINT `fk_Ocupacao_Jogo` FOREIGN KEY (`idJogo`) REFERENCES `Jogo` (`idJogo`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `Sound`
  ADD CONSTRAINT `fk_Sound_Jogo` FOREIGN KEY (`idJogo`) REFERENCES `Jogo` (`idJogo`) ON DELETE CASCADE ON UPDATE CASCADE;

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
CREATE DEFINER=`root`@`%` PROCEDURE `Criar_utilizador`(
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
    UPDATE Users
    SET nome = p_nome,
        telemovel = p_telemovel,
        tipo = p_tipo,
        grupo = p_grupo,
        ativo = TRUE
    WHERE email = p_email;
    ELSE
        -- Insere dados na tabela Users (SEM armazenar senha)
        INSERT INTO Users (email, nome, telemovel, tipo, grupo, ativo)
        VALUES (p_email, p_nome, p_telemovel, p_tipo, p_grupo, TRUE);

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

    -- Concede acesso EXECUTE a todos os SPs no schema
    SET @sql_sp_access = CONCAT('GRANT EXECUTE ON mydb.* TO \'', v_username, '\'@\'%\'');
    PREPARE sp_stmt FROM @sql_sp_access;
    EXECUTE sp_stmt;
    DEALLOCATE PREPARE sp_stmt;

    -- Concede acesso aos triggers
    SET @sql_trigger_access = CONCAT('GRANT TRIGGER ON mydb.* TO \'', v_username, '\'@\'%\'');
    PREPARE trigger_stmt FROM @sql_trigger_access;
    EXECUTE trigger_stmt;
    DEALLOCATE PREPARE trigger_stmt;


    -- Remove o acesso geral às tabelas e concede acesso apenas às views específicas
    IF p_tipo = 'player' THEN
        SET @sql_view_access = CONCAT('GRANT SELECT ON mydb.vw_player_dados TO \'', v_username, '\'@\'%\'');
    ELSEIF p_tipo = 'tester' THEN
        SET @sql_view_access = CONCAT('GRANT SELECT ON mydb.vw_tester_dados TO \'', v_username, '\'@\'%\'');
    ELSE
        -- Admins mantêm acesso completo
        SET @sql_view_access = CONCAT('GRANT SELECT ON mydb.* TO \'', v_username, '\'@\'%\'');
    END IF;

    PREPARE view_stmt FROM @sql_view_access;
    EXECUTE view_stmt;
    DEALLOCATE PREPARE view_stmt;

    -- Atualiza privilégios
    FLUSH PRIVILEGES;

    END IF;

END $$

DELIMITER ;


DELIMITER $$

CREATE DEFINER='root'@'%' PROCEDURE `Remover_jogo`(
	IN p_id_jogo INT
)
BEGIN
	IF (SELECT estado FROM Jogo WHERE idJogo = p_ID_Jogo) = 0 THEN
		DELETE FROM Jogo WHERE idJogo = p_id_jogo;
	ELSE
		SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Erro: Este jogo está a decorrer';
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

        -- Desativa o utilizador na tabela Users ativo a FALSE
    UPDATE Users
        SET ativo = FALSE
    WHERE email = p_email;

    END IF;
END$$

DELIMITER ;


DELIMITER $$

CREATE PROCEDURE startGame(
	IN p_id_jogo INT
)
BEGIN
	UPDATE Jogo
    SET estado = 1
    WHERE idJogo = p_id_jogo;
END$$

DELIMITER ;


DELIMITER $$

CREATE PROCEDURE endGame(
	IN p_id_jogo INT
)
BEGIN
	UPDATE Jogo
    SET estado = 0
    WHERE idJogo = p_id_jogo;
END$$

DELIMITER ;

DELIMITER $$

CREATE DEFINER=`root`@`%` PROCEDURE `Alterar_jogo_admin`(
    IN p_idJogo INT,
    IN p_descricao TEXT,
    IN p_jogador VARCHAR(100),
    IN p_scoreTotal INT,
    IN p_dataHoraInicio DATETIME,
    IN p_estado TINYINT
)
BEGIN
    DECLARE v_userType VARCHAR(10);
    DECLARE v_gameIsRunning BOOLEAN;
    DECLARE v_emailJogo VARCHAR(50);
    DECLARE v_oldEmail VARCHAR(50);

    -- Verifica se o jogo existe e obtém o proprietário e estado
    SELECT estado INTO v_gameIsRunning
    FROM Jogo WHERE idJogo = p_idJogo;

    -- Obtém o tipo de usuário que está tentando modificar
    SELECT tipo INTO v_userType FROM Users WHERE nome = SUBSTRING_INDEX(SESSION_USER(), '@', 1);

    IF v_userType IS NULL THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Erro: Usuário não encontrado.';

    -- Administrador pode alterar tudo
    ELSEIF v_userType = 'admin' THEN
    IF NOT v_gameIsRunning THEN

    UPDATE Jogo
    SET
        descricao = IFNULL(p_descricao, descricao),
        jogador = IFNULL(p_jogador, jogador),
        scoreTotal = IFNULL(p_scoreTotal, scoreTotal),
        dataHoraInicio = IFNULL(p_dataHoraInicio, dataHoraInicio),
        estado = IF(p_estado IS NULL, estado, p_estado)
    WHERE idJogo = p_idJogo;
    ELSE
                SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'Erro: Jogo em execução não pode ser alterado.';
    END IF;

        -- Dono do jogo (player ou tester) pode alterar alguns campos
        ELSEIF v_userType = 'player' OR v_userType = 'Tester' THEN

            IF NOT v_gameIsRunning THEN
                UPDATE Jogo
                    SET
                        descricao = IFNULL(p_descricao, descricao),
                        jogador = IFNULL(p_jogador, jogador)
                    WHERE idJogo = p_idJogo;
            ELSE
                SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'Erro: Jogo em execução não pode ser alterado.';
            END IF;

        ELSE
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Erro: Permissão negada para modificar este jogo.';
    END IF;
END$$

DELIMITER ;


DELIMITER $$
    CREATE DEFINER=`root`@`%` PROCEDURE `Alterar_jogo`(
    IN p_idJogo INT,
    IN p_descricao TEXT
)
BEGIN
    DECLARE v_userType VARCHAR(10);
    DECLARE v_gameIsRunning BOOLEAN;
    DECLARE v_userEmail VARCHAR(255);
    DECLARE v_jogoEmail VARCHAR(255);

    -- Obtém o email do usuário atual (parte antes do @)
    SET v_userEmail = CONCAT(SUBSTRING_INDEX(USER(), '@', 1), '%');

    -- Verifica se o jogo existe e obtém o estado e email do proprietário
    SELECT estado, email INTO v_gameIsRunning, v_jogoEmail
    FROM Jogo WHERE idJogo = p_idJogo;

    -- Verifica se o jogo existe
    IF v_jogoEmail IS NULL THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Erro: Jogo não encontrado.';
    END IF;

    -- Obtém o tipo de usuário que está tentando modificar
    SELECT tipo INTO v_userType
    FROM Users
    WHERE email LIKE v_userEmail;

    -- Verifica se o usuário existe e é tester ou player
    IF v_userType IS NULL THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Erro: Usuário não encontrado.';
    ELSEIF v_userType NOT IN ('tester', 'player') THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Erro: Apenas testers e players podem alterar jogos.';
    END IF;

    -- Verifica se o usuário é o dono do jogo
    IF NOT EXISTS (SELECT 1 FROM Users WHERE email LIKE v_userEmail AND email = v_jogoEmail) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Erro: Você só pode alterar seus próprios jogos.';
    END IF;

    -- Verifica se o jogo está em execução
    IF v_gameIsRunning THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Erro: Jogo em execução não pode ser alterado.';
    END IF;

    -- Atualiza a descrição do jogo
    UPDATE Jogo
    SET descricao = IFNULL(p_descricao, descricao)
    WHERE idJogo = p_idJogo;
END$$

DELIMITER ;


DELIMITER $$

CREATE DEFINER='root'@'%' PROCEDURE Criar_jogo_admin(
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

    -- Insere o novo jogo
    INSERT INTO Jogo (email, descricao, jogador, scoreTotal, dataHoraInicio, estado)
    VALUES (p_email, p_descricao, p_jogador, 0, p_dataHoraInicio, FALSE);
END$$

DELIMITER ;


DELIMITER $$

CREATE DEFINER=`root`@`%` PROCEDURE `Criar_jogo`(
    IN p_descricao TEXT
)
BEGIN
    DECLARE v_user_type VARCHAR(20);
    DECLARE v_email VARCHAR(255);
    DECLARE v_username VARCHAR(50);
    DECLARE user_count INT;

    -- Obtém apenas o nome do usuário (parte antes do @)
    SET v_username = SUBSTRING_INDEX(USER(), '@', 1);

    -- Conta quantos usuários começam com este username (independente do domínio)
    SELECT COUNT(*) INTO user_count FROM Users
    WHERE email LIKE CONCAT(v_username, '@%');

    -- Verifica se encontrou exatamente um usuário
    IF user_count = 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Erro: Nenhum usuário encontrado com este nome';
    ELSEIF user_count > 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Erro: Múltiplos usuários encontrados com este nome';
    END IF;

    -- Obtém o email completo do usuário
    SELECT email INTO v_email FROM Users
    WHERE email LIKE CONCAT(v_username, '@%') LIMIT 1;

    -- Obtém o tipo de utilizador
    SELECT tipo INTO v_user_type FROM Users WHERE email = v_email;

    -- Verifica se o usuário tem permissão para criar jogos
    IF v_user_type NOT IN ('admin', 'player', 'tester') THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Erro: Usuário não tem permissão para criar jogos';
    END IF;

        -- Insere o novo jogo
    INSERT INTO Jogo (email, descricao, jogador, scoreTotal, dataHoraInicio, estado)
    VALUES (v_email, p_descricao, NULL, 0, NULL, FALSE);

END$$

DELIMITER ;

# - - administrador - -
# TABLES
-- Conceda acesso ao schema inteiro (substitua 'meu_schema' pelo nome correto)
GRANT SELECT, INSERT, UPDATE, DELETE ON mydb.* TO 'admin';
GRANT SELECT, INSERT, UPDATE, DELETE ON Jogo TO "admin";
GRANT SELECT, INSERT, UPDATE, DELETE ON MedicaoPassagem TO "admin";
GRANT SELECT, INSERT, UPDATE, DELETE ON OcupacaoLabirinto TO "admin";
GRANT SELECT, INSERT, UPDATE, DELETE ON Sound TO "admin";
GRANT SELECT, INSERT, UPDATE, DELETE ON Users TO "admin";
GRANT SELECT, INSERT, UPDATE, DELETE ON Mensagens TO "admin";
# STORED PROCEDURES
GRANT EXECUTE ON PROCEDURE Criar_utilizador TO "admin";
GRANT EXECUTE ON PROCEDURE Alterar_utilizador TO "admin";
GRANT EXECUTE ON PROCEDURE Remover_utilizador TO "admin";
GRANT EXECUTE ON PROCEDURE Criar_jogo TO "admin";
GRANT EXECUTE ON PROCEDURE Alterar_jogo TO "admin";
GRANT EXECUTE ON PROCEDURE Remover_jogo TO "admin";

# - - jogador - -
# TABLES
GRANT SELECT, INSERT, UPDATE ON Jogo TO "player";
GRANT SELECT, INSERT ON MedicaoPassagem TO "player";
GRANT SELECT, INSERT ON Mensagens TO "player";
GRANT SELECT, INSERT, UPDATE ON OcupacaoLabirinto TO "player";
GRANT SELECT, INSERT ON Sound TO "player";
GRANT SELECT, UPDATE ON Users TO "player";
GRANT SELECT, INSERT ON Mensagens TO "player";
# STORED PROCEDURES
GRANT EXECUTE ON PROCEDURE startGame TO "player";
GRANT EXECUTE ON PROCEDURE getPoints TO "player";
GRANT EXECUTE ON PROCEDURE endGame TO "player";
GRANT EXECUTE ON PROCEDURE Criar_jogo TO "player";
GRANT EXECUTE ON PROCEDURE Alterar_jogo TO "player";

# - - tester - -
# TABLES
GRANT SELECT ON Jogo TO "tester";
GRANT SELECT ON MedicaoPassagem TO "tester";
GRANT SELECT ON Mensagens TO "tester";
GRANT SELECT ON OcupacaoLabirinto TO "tester";
GRANT SELECT ON Sound TO "tester";
GRANT SELECT ON Mensagens TO "tester";
GRANT SELECT, UPDATE ON Users TO "tester";
# STORED PROCEDURES
GRANT EXECUTE ON PROCEDURE endGame TO "tester";
GRANT EXECUTE ON PROCEDURE startGame TO "tester";
GRANT EXECUTE ON PROCEDURE getPoints TO "tester";
GRANT EXECUTE ON PROCEDURE Alterar_utilizador TO "tester";
GRANT EXECUTE ON PROCEDURE Criar_jogo TO "tester";
GRANT EXECUTE ON PROCEDURE Alterar_jogo TO "tester";
