SELECT NVL(CORTE.CODFILIAL, FALTA.CODFILIAL)                               AS CODFILIAL,
       TO_CHAR(TRUNC(SYSDATE - 1), 'DD/MM/YYYY')                           AS DATA,
       PCPRODUT.CODPROD                                                    AS CODPROD,
       PCPRODUT.DESCRICAO                                                  AS DESCRICAO,

       -- QUANTIDADE DE FALTA
       NVL(FALTA.TOTAL_QT_FALTA, 0)                                        AS QT_FALTA,

       -- VALOR DE VENDA DA FALTA
       NVL(FALTA.TOTAL_PVENDA_FALTA, 0)                                    AS PVENDA_FALTA,

       NVL(FALTA.CNT_NUMPED_FALTA, 0)                                      AS COUNT_PED_FALTA,

       -- QUANTIDADE DE CORTE
       NVL(CORTE.TOTAL_QT_CORTE, 0)                                        AS QT_CORTE,

       -- VALOR DE VENDA DO CORTE
       NVL(CORTE.TOTAL_PVENDA_CORTE, 0)                                    AS PVENDA_CORTE,

       NVL(CORTE.CNT_NUMPED_CORTE, 0)                                      AS COUNT_PED_CORTE,

       -- INFORMAÇÕES DE ESTOQUE DIRETAMENTE NO SELECT
       PKG_ESTOQUE.ESTOQUE_DISPONIVEL(PCEST.CODPROD, PCEST.CODFILIAL, 'V') AS DISP,
       PCEST.QTEST                                                         AS QT_CONTABIL,
       PCEST.QTBLOQUEADA,
       PCEST.QTINDENIZ

FROM PCPRODUT
         INNER JOIN PCEST
                    ON PCEST.CODPROD = PCPRODUT.CODPROD

    -- SUBCONSULTA PARA AGREGAÇÃO DE PCCORTEI (CORTE)
         LEFT JOIN (SELECT CODFILIAL,
                           CODPROD,
                           SUM(QTCORTADA)                    AS TOTAL_QT_CORTE,
                           ROUND(SUM(PVENDA * QTCORTADA), 2) AS TOTAL_PVENDA_CORTE,
                           COUNT(NUMPED)                     AS CNT_NUMPED_CORTE
                    FROM PCCORTEI
                    WHERE DATA = TRUNC(SYSDATE - 1)
                    GROUP BY CODFILIAL,
                             CODPROD) CORTE
                   ON CORTE.CODPROD = PCPRODUT.CODPROD
                       AND CORTE.CODFILIAL = PCEST.CODFILIAL

    -- SUBCONSULTA PARA AGREGAÇÃO DE PCFALTA (FALTA)
         LEFT JOIN (SELECT CODFILIAL,
                           CODPROD,
                           SUM(QT)                    AS TOTAL_QT_FALTA,
                           ROUND(SUM(PVENDA * QT), 2) AS TOTAL_PVENDA_FALTA,
                           COUNT(NUMPED)              AS CNT_NUMPED_FALTA
                    FROM PCFALTA
                    WHERE DATA = TRUNC(SYSDATE - 1)
                    GROUP BY CODFILIAL,
                             CODPROD) FALTA
                   ON FALTA.CODPROD = PCPRODUT.CODPROD
                       AND FALTA.CODFILIAL = PCEST.CODFILIAL

WHERE (NVL(FALTA.CNT_NUMPED_FALTA, 0) > 0 OR NVL(CORTE.CNT_NUMPED_CORTE, 0) > 0)

ORDER BY (NVL(FALTA.TOTAL_PVENDA_FALTA, 0) + NVL(CORTE.TOTAL_PVENDA_CORTE, 0)) DESC,
         TO_CHAR(TRUNC(SYSDATE - 1), 'DD/MM/YYYY') DESC,
         PCPRODUT.DESCRICAO