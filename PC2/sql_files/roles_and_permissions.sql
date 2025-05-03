CREATE ROLE IF NOT EXISTS "jogador";

# SELECT
GRANT SELECT ON Jogo TO jogador;
GRANT SELECT ON Sound TO jogador;
GRANT SELECT ON MedicaoPassagem TO jogador;
GRANT SELECT ON Mensagens TO jogador;
GRANT SELECT ON OcupacaoLabirinto TO jogador;

# UPDATE
GRANT UPDATE ON Jogo TO jogador;
GRANT UPDATE ON User TO jogador;
GRANT UPDATE ON OcupacaoLabirinto TO jogador;

# INSERT
GRANT INSERT ON Jogo TO jogador;
GRANT INSERT ON Sound TO jogador;
GRANT INSERT ON Mensagens TO jogador;
GRANT INSERT ON MedicaoPassagem TO jogador;
GRANT INSERT ON OcupacaoLabirinto TO jogador;