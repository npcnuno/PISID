DELIMITER //
CREATE PROCEDURE Alterar_utilizador(
    IN p_Email VARCHAR(50),
    IN p_Nome VARCHAR(100),
    IN p_Telemovel VARCHAR(12),
    #IN p_Tipo ENUM('jog', 'tester'),
    IN p_Tipo VARCHAR(3),
    #IN p_Grupo VARCHAR(50)
    IN p_grupo INT
)
BEGIN
        UPDATE User 
        SET 
            nome = p_Nome,
            telemovel = p_Telemovel,
            tipo = p_Tipo,
            grupo = p_Grupo
        WHERE email = p_Email;
END //
DELIMITER ;