
-- initdb/01-schema.sql

CREATE DATABASE IF NOT EXISTS labirinto_mysql;
USE labirinto_mysql;

-- Jogo
CREATE TABLE IF NOT EXISTS Jogo (
  IDJogo INT AUTO_INCREMENT PRIMARY KEY,
  Descricao TEXT,
  jogador VARCHAR(50),
  DataHoraInicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- MedicoesPassagens
CREATE TABLE IF NOT EXISTS MedicoesPassagens (
  IDMedicao INT AUTO_INCREMENT PRIMARY KEY,
  Hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  SalaOrigem INT,
  SalaDestino INT,
  Marsami INT,
  Status INT
);

-- Utilizador
CREATE TABLE IF NOT EXISTS Utilizador (
  Nome VARCHAR(100),
  Telemovel VARCHAR(12),
  Tipo VARCHAR(3),
  Email VARCHAR(50)
);

-- Mensagens
CREATE TABLE IF NOT EXISTS Mensagens (
  ID INT AUTO_INCREMENT PRIMARY KEY,
  Hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  Sala INT,
  Sensor INT,
  Leitura DECIMAL(6,2),
  TipoArea INT,
  MsgExtra VARCHAR(255),
  HoraEscrita TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- OcupacaoLabirinto
CREATE TABLE IF NOT EXISTS OcupacaoLabirinto (
  IDJogo INT,
  NumeroMarsamisOdd INT,
  NumeroMarsamisEven INT,
  Sala INT
);

-- Any additional tables can go here
