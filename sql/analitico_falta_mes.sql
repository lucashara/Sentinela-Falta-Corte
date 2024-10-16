SELECT TO_CHAR(PCFALTA.DATA, 'DD/MM/YYYY')                   AS DATA,
       PCFALTA.CODFILIAL                                     AS CODFILIAL,
       PCFALTA.NUMPED                                        AS NUMPED,
       PCPEDC.POSICAO,
       UPPER(PCPEDC.CODSUPERVISOR || ' - ' || PCSUPERV.NOME) AS SUPERVISAO,
       PCPEDC.CODUSUR || ' - ' || PCUSUARI.NOME              AS RCA,
       PCPRODUT.CODPROD                                      AS CODPROD,
       PCPRODUT.DESCRICAO                                    AS DESCRICAO,

       -- QUANTIDADE E VALOR DE CORTE
       PCFALTA.QT                                            AS QT_FALTA,
       ROUND(PCFALTA.QT * PCFALTA.PVENDA, 2)                 AS PVENDA_FALTA

FROM PCPRODUT
         INNER JOIN PCEST
                    ON PCEST.CODPROD = PCPRODUT.CODPROD

         INNER JOIN PCFALTA ON PCFALTA.CODPROD = PCPRODUT.CODPROD AND PCFALTA.CODFILIAL = PCEST.CODFILIAL

    -- JUNÇÃO COM PCPEDC PARA OBTER POSIÇÃO, SUPERVISAO E RCA
         LEFT JOIN PCPEDC
                   ON PCPEDC.NUMPED = PCFALTA.NUMPED AND PCPEDC.CODFILIAL = PCFALTA.CODFILIAL

         LEFT JOIN PCSUPERV
                   ON PCSUPERV.CODSUPERVISOR = PCPEDC.CODSUPERVISOR

         LEFT JOIN PCUSUARI
                   ON PCUSUARI.CODUSUR = PCPEDC.CODUSUR

WHERE PCFALTA.NUMPED IS NOT NULL
  AND PCFALTA.DATA >= TRUNC(SYSDATE, 'MM')

ORDER BY TO_CHAR(PCFALTA.DATA, 'DD/MM/YYYY') DESC,
         PCFALTA.CODFILIAL,
         PCFALTA.QT * PCFALTA.PVENDA DESC,
         PCPRODUT.DESCRICAO
