/* SINTÉTICO DE CORTE — ONTEM (POR PRODUTO E FILIAL)
   OBSERVAÇÃO: PARAMETRIZADO COM :DATAI E :DATAF (PASSE AMBOS = ONTEM).
*/
SELECT NVL(CORTE.CODFILIAL, PCEST.CODFILIAL)                               AS CODFILIAL,
       PCPRODUT.CODPROD                                                    AS CODPROD,
       PCPRODUT.DESCRICAO                                                  AS DESCRICAO,
       NVL(CORTE.TOTAL_QT_CORTE, 0)                                        AS QT_CORTE,
       NVL(CORTE.TOTAL_PVENDA_CORTE, 0)                                    AS PVENDA_CORTE,
       NVL(CORTE.CNT_NUMPED_CORTE, 0)                                      AS COUNT_PED_CORTE,
       PKG_ESTOQUE.ESTOQUE_DISPONIVEL(PCEST.CODPROD, PCEST.CODFILIAL, 'V') AS DISP,
       PCEST.QTEST                                                         AS QT_CONTABIL,
       PCEST.QTBLOQUEADA,
       PCEST.QTINDENIZ
FROM PCPRODUT
         JOIN PCEST ON PCEST.CODPROD = PCPRODUT.CODPROD
         LEFT JOIN (SELECT CODFILIAL,
                           CODPROD,
                           SUM(QTCORTADA)                    AS TOTAL_QT_CORTE,
                           ROUND(SUM(PVENDA * QTCORTADA), 2) AS TOTAL_PVENDA_CORTE,
                           COUNT(DISTINCT NUMPED)            AS CNT_NUMPED_CORTE
                    FROM PCCORTEI
                    WHERE DATA BETWEEN :DATAI AND :DATAF
                      AND NUMPED IS NOT NULL
                    GROUP BY CODFILIAL, CODPROD) CORTE
                   ON CORTE.CODPROD = PCPRODUT.CODPROD
                       AND CORTE.CODFILIAL = PCEST.CODFILIAL
WHERE NVL(CORTE.CNT_NUMPED_CORTE, 0) > 0
ORDER BY NVL(CORTE.TOTAL_PVENDA_CORTE, 0) DESC, PCPRODUT.DESCRICAO