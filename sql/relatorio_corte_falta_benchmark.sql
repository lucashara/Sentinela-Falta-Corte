/*-----------------------------------------------------------
   RELATÓRIO DIÁRIO – CORTE, FALTA E FATURAMENTO
   PERÍODO :DATAI–:DATAF

   CORTE: COMPARA % DO PERÍODO COM META FIXA 0,03%
   FALTA: COMPARA % DO PERÍODO COM MÉDIA TRIMESTRAL (ÚLTIMOS 91 DIAS)
-----------------------------------------------------------*/
WITH PARAM AS (SELECT :DATAI                             AS DATAI,
                      :DATAF                             AS DATAF,
                      TRUNC(SYSDATE) - 1                 AS TRIM_FIM,
                      TRUNC(SYSDATE) - INTERVAL '90' DAY AS TRIM_INI
               FROM DUAL),

/* FILIAIS QUE TIVERAM MOVIMENTO (CORTE/FALTA/FATURAMENTO) NO PERÍODO */
     FILIAIS AS (SELECT CODFILIAL
                 FROM PCCORTEI,
                      PARAM
                 WHERE DATA BETWEEN PARAM.DATAI AND PARAM.DATAF
                 UNION
                 SELECT CODFILIAL
                 FROM PCFALTA,
                      PARAM
                 WHERE DATA BETWEEN PARAM.DATAI AND PARAM.DATAF
                 UNION
                 SELECT CODFILIAL
                 FROM FARMAUM.VIEW_1464_MATERIALIZED,
                      PARAM
                 WHERE DATA BETWEEN PARAM.DATAI AND PARAM.DATAF),

/* % CORTE POR DIA NO PERÍODO SELECIONADO */
     DIARIO_PERIODO_CORTE AS (SELECT CODFILIAL,
                                     DATA,
                                     CASE
                                         WHEN SUM(VLVENDA) = 0 THEN 0
                                         ELSE ROUND(SUM(QTCORTADA * PVENDA) / SUM(VLVENDA) * 100, 2)
                                         END AS PCT_CORTE
                              FROM (SELECT CODFILIAL, DATA, QTCORTADA, PVENDA, 0 AS VLVENDA
                                    FROM PCCORTEI,
                                         PARAM
                                    WHERE DATA BETWEEN PARAM.DATAI AND PARAM.DATAF
                                    UNION ALL
                                    SELECT CODFILIAL, DATA, 0 AS QTCORTADA, 0 AS PVENDA, VLVENDA
                                    FROM FARMAUM.VIEW_1464_MATERIALIZED,
                                         PARAM
                                    WHERE DATA BETWEEN PARAM.DATAI AND PARAM.DATAF)
                              GROUP BY CODFILIAL, DATA),

/* % MÉDIA DO PERÍODO (EXIBIÇÃO) */
     MEDIA_PERIODO_CORTE AS (SELECT CODFILIAL, ROUND(AVG(PCT_CORTE), 2) AS MEDIA_PERIODO_CORTE
                             FROM DIARIO_PERIODO_CORTE
                             GROUP BY CODFILIAL),

/* % FALTA POR DIA NO PERÍODO SELECIONADO */
     DIARIO_PERIODO_FALTA AS (SELECT CODFILIAL,
                                     DATA,
                                     CASE
                                         WHEN SUM(VLVENDA) = 0 THEN 0
                                         ELSE ROUND(SUM(QT * PVENDA) / SUM(VLVENDA) * 100, 2)
                                         END AS PCT_FALTA
                              FROM (SELECT CODFILIAL, DATA, QT, PVENDA, 0 AS VLVENDA
                                    FROM PCFALTA,
                                         PARAM
                                    WHERE DATA BETWEEN PARAM.DATAI AND PARAM.DATAF
                                    UNION ALL
                                    SELECT CODFILIAL, DATA, 0 AS QT, 0 AS PVENDA, VLVENDA
                                    FROM FARMAUM.VIEW_1464_MATERIALIZED,
                                         PARAM
                                    WHERE DATA BETWEEN PARAM.DATAI AND PARAM.DATAF)
                              GROUP BY CODFILIAL, DATA),

     MEDIA_PERIODO_FALTA AS (SELECT CODFILIAL, ROUND(AVG(PCT_FALTA), 2) AS MEDIA_PERIODO_FALTA
                             FROM DIARIO_PERIODO_FALTA
                             GROUP BY CODFILIAL),

/* TRIMESTRE MÓVEL DA FALTA (MANTIDO) */
     DIARIO_TRIM_FALTA AS (SELECT CODFILIAL,
                                  DATA,
                                  CASE
                                      WHEN SUM(VLVENDA) = 0 THEN 0
                                      ELSE ROUND(SUM(QT * PVENDA) / SUM(VLVENDA) * 100, 2)
                                      END AS PCT_FALTA
                           FROM (SELECT CODFILIAL, DATA, QT, PVENDA, 0 AS VLVENDA
                                 FROM PCFALTA,
                                      PARAM
                                 WHERE DATA BETWEEN PARAM.TRIM_INI AND PARAM.TRIM_FIM
                                 UNION ALL
                                 SELECT CODFILIAL, DATA, 0 AS QT, 0 AS PVENDA, VLVENDA
                                 FROM FARMAUM.VIEW_1464_MATERIALIZED,
                                      PARAM
                                 WHERE DATA BETWEEN PARAM.TRIM_INI AND PARAM.TRIM_FIM)
                           GROUP BY CODFILIAL, DATA),

     MEDIA_TRIM_FALTA AS (SELECT CODFILIAL, ROUND(AVG(PCT_FALTA), 2) AS MEDIA_TRIM_FALTA
                          FROM DIARIO_TRIM_FALTA
                          GROUP BY CODFILIAL),

/* DADOS MONETÁRIOS NO PERÍODO */
     DADOS_CORTE AS (SELECT CODFILIAL,
                            ROUND(
                                    NVL((SELECT SUM(QTCORTADA * PVENDA)
                                         FROM PCCORTEI,
                                              PARAM
                                         WHERE DATA BETWEEN PARAM.DATAI AND PARAM.DATAF
                                           AND CODFILIAL = FILIAIS.CODFILIAL), 0), 2
                            ) AS PVENDA_CORTE
                     FROM FILIAIS),

     DADOS_FALTA AS (SELECT CODFILIAL,
                            ROUND(
                                    NVL((SELECT SUM(QT * PVENDA)
                                         FROM PCFALTA,
                                              PARAM
                                         WHERE DATA BETWEEN PARAM.DATAI AND PARAM.DATAF
                                           AND CODFILIAL = FILIAIS.CODFILIAL), 0), 2
                            ) AS PVENDA_FALTA
                     FROM FILIAIS),

     DADOS_FATURAMENTO AS (SELECT CODFILIAL,
                                  ROUND(
                                          NVL((SELECT SUM(VLVENDA)
                                               FROM FARMAUM.VIEW_1464_MATERIALIZED,
                                                    PARAM
                                               WHERE DATA BETWEEN PARAM.DATAI AND PARAM.DATAF
                                                 AND CODFILIAL = FILIAIS.CODFILIAL), 0), 2
                                  ) AS FATURAMENTO
                           FROM FILIAIS),

/* ====== META FIXA DE CORTE ====== */
     META_CORTE AS (SELECT CODFILIAL, 0.03 AS META_CORTE
                    FROM FILIAIS),

     RESULT AS (
         /* ---- POR FILIAL ---- */
         SELECT DADOS_CORTE.CODFILIAL,
                DADOS_CORTE.PVENDA_CORTE,
                TO_CHAR(MEDIA_PERIODO_CORTE.MEDIA_PERIODO_CORTE, 'FM990D00') || '%' AS PCT_PERIODO_CORTE,
                TO_CHAR(META_CORTE.META_CORTE, 'FM990D00') || '%'                   AS META_CORTE,
                CASE
                    WHEN MEDIA_PERIODO_CORTE.MEDIA_PERIODO_CORTE > META_CORTE.META_CORTE
                        THEN '+' ||
                             TO_CHAR(MEDIA_PERIODO_CORTE.MEDIA_PERIODO_CORTE - META_CORTE.META_CORTE, 'FM990D00') ||
                             '% ACIMA'
                    WHEN MEDIA_PERIODO_CORTE.MEDIA_PERIODO_CORTE < META_CORTE.META_CORTE
                        THEN '-' ||
                             TO_CHAR(META_CORTE.META_CORTE - MEDIA_PERIODO_CORTE.MEDIA_PERIODO_CORTE, 'FM990D00') ||
                             '% ABAIXO'
                    ELSE '0% (NA META)'
                    END                                                             AS DESVIO_CORTE,
                DADOS_FALTA.PVENDA_FALTA,
                TO_CHAR(MEDIA_PERIODO_FALTA.MEDIA_PERIODO_FALTA, 'FM990D00') || '%' AS PCT_PERIODO_FALTA,
                TO_CHAR(MEDIA_TRIM_FALTA.MEDIA_TRIM_FALTA, 'FM990D00') || '%'       AS MEDIA_TRIM_FALTA,
                CASE
                    WHEN MEDIA_PERIODO_FALTA.MEDIA_PERIODO_FALTA > MEDIA_TRIM_FALTA.MEDIA_TRIM_FALTA
                        THEN '+' || TO_CHAR(MEDIA_PERIODO_FALTA.MEDIA_PERIODO_FALTA - MEDIA_TRIM_FALTA.MEDIA_TRIM_FALTA,
                                            'FM990D00') || '% ACIMA'
                    WHEN MEDIA_PERIODO_FALTA.MEDIA_PERIODO_FALTA < MEDIA_TRIM_FALTA.MEDIA_TRIM_FALTA
                        THEN '-' || TO_CHAR(MEDIA_TRIM_FALTA.MEDIA_TRIM_FALTA - MEDIA_PERIODO_FALTA.MEDIA_PERIODO_FALTA,
                                            'FM990D00') || '% ABAIXO'
                    ELSE '0% (NA MÉDIA)'
                    END                                                             AS DESVIO_FALTA,
                DADOS_FATURAMENTO.FATURAMENTO,
                DADOS_CORTE.PVENDA_CORTE + DADOS_FALTA.PVENDA_FALTA                 AS MOV,
                0                                                                   AS ORD
         FROM DADOS_CORTE
                  JOIN MEDIA_PERIODO_CORTE ON DADOS_CORTE.CODFILIAL = MEDIA_PERIODO_CORTE.CODFILIAL
                  JOIN META_CORTE ON DADOS_CORTE.CODFILIAL = META_CORTE.CODFILIAL
                  JOIN DADOS_FALTA ON DADOS_CORTE.CODFILIAL = DADOS_FALTA.CODFILIAL
                  JOIN MEDIA_PERIODO_FALTA ON DADOS_CORTE.CODFILIAL = MEDIA_PERIODO_FALTA.CODFILIAL
                  JOIN MEDIA_TRIM_FALTA ON DADOS_CORTE.CODFILIAL = MEDIA_TRIM_FALTA.CODFILIAL
                  JOIN DADOS_FATURAMENTO ON DADOS_CORTE.CODFILIAL = DADOS_FATURAMENTO.CODFILIAL

         UNION ALL

         /* ---- LINHA TOTAL ---- */
         SELECT 'TOTAL'                                                                      AS CODFILIAL,
                SUM(DADOS_CORTE.PVENDA_CORTE)                                                AS PVENDA_CORTE,
                TO_CHAR(
                        ROUND(SUM(DADOS_CORTE.PVENDA_CORTE) / NULLIF(SUM(DADOS_FATURAMENTO.FATURAMENTO), 0) * 100, 2),
                        'FM990D00') || '%'                                                   AS PCT_PERIODO_CORTE,
                TO_CHAR(ROUND(AVG(META_CORTE.META_CORTE), 2), 'FM990D00') || '%'             AS META_CORTE,
                CASE
                    WHEN ROUND(SUM(DADOS_CORTE.PVENDA_CORTE) / NULLIF(SUM(DADOS_FATURAMENTO.FATURAMENTO), 0) * 100, 2)
                        > AVG(META_CORTE.META_CORTE)
                        THEN '+' || TO_CHAR(
                            ROUND(SUM(DADOS_CORTE.PVENDA_CORTE) / NULLIF(SUM(DADOS_FATURAMENTO.FATURAMENTO), 0) * 100,
                                  2)
                                - AVG(META_CORTE.META_CORTE),
                            'FM990D00') || '% ACIMA'
                    WHEN ROUND(SUM(DADOS_CORTE.PVENDA_CORTE) / NULLIF(SUM(DADOS_FATURAMENTO.FATURAMENTO), 0) * 100, 2)
                        < AVG(META_CORTE.META_CORTE)
                        THEN '-' || TO_CHAR(
                            AVG(META_CORTE.META_CORTE)
                                -
                            ROUND(SUM(DADOS_CORTE.PVENDA_CORTE) / NULLIF(SUM(DADOS_FATURAMENTO.FATURAMENTO), 0) * 100,
                                  2),
                            'FM990D00') || '% ABAIXO'
                    ELSE '0% (NA META)'
                    END                                                                      AS DESVIO_CORTE,
                SUM(DADOS_FALTA.PVENDA_FALTA)                                                AS PVENDA_FALTA,
                TO_CHAR(
                        ROUND(SUM(DADOS_FALTA.PVENDA_FALTA) / NULLIF(SUM(DADOS_FATURAMENTO.FATURAMENTO), 0) * 100, 2),
                        'FM990D00') || '%'                                                   AS PCT_PERIODO_FALTA,
                TO_CHAR(ROUND(AVG(MEDIA_TRIM_FALTA.MEDIA_TRIM_FALTA), 2), 'FM990D00') || '%' AS MEDIA_TRIM_FALTA,
                CASE
                    WHEN ROUND(SUM(DADOS_FALTA.PVENDA_FALTA) / NULLIF(SUM(DADOS_FATURAMENTO.FATURAMENTO), 0) * 100, 2)
                        > AVG(MEDIA_TRIM_FALTA.MEDIA_TRIM_FALTA)
                        THEN '+' || TO_CHAR(
                            ROUND(SUM(DADOS_FALTA.PVENDA_FALTA) / NULLIF(SUM(DADOS_FATURAMENTO.FATURAMENTO), 0) * 100,
                                  2)
                                - AVG(MEDIA_TRIM_FALTA.MEDIA_TRIM_FALTA),
                            'FM990D00') || '% ACIMA'
                    WHEN ROUND(SUM(DADOS_FALTA.PVENDA_FALTA) / NULLIF(SUM(DADOS_FATURAMENTO.FATURAMENTO), 0) * 100, 2)
                        < AVG(MEDIA_TRIM_FALTA.MEDIA_TRIM_FALTA)
                        THEN '-' || TO_CHAR(
                            AVG(MEDIA_TRIM_FALTA.MEDIA_TRIM_FALTA)
                                -
                            ROUND(SUM(DADOS_FALTA.PVENDA_FALTA) / NULLIF(SUM(DADOS_FATURAMENTO.FATURAMENTO), 0) * 100,
                                  2),
                            'FM990D00') || '% ABAIXO'
                    ELSE '0% (NA MÉDIA)'
                    END                                                                      AS DESVIO_FALTA,
                SUM(DADOS_FATURAMENTO.FATURAMENTO)                                           AS FATURAMENTO,
                SUM(DADOS_CORTE.PVENDA_CORTE) + SUM(DADOS_FALTA.PVENDA_FALTA)                AS MOV,
                1                                                                            AS ORD
         FROM DADOS_CORTE
                  JOIN DADOS_FALTA ON DADOS_CORTE.CODFILIAL = DADOS_FALTA.CODFILIAL
                  JOIN DADOS_FATURAMENTO ON DADOS_CORTE.CODFILIAL = DADOS_FATURAMENTO.CODFILIAL
                  JOIN META_CORTE ON DADOS_CORTE.CODFILIAL = META_CORTE.CODFILIAL
                  JOIN MEDIA_TRIM_FALTA ON DADOS_CORTE.CODFILIAL = MEDIA_TRIM_FALTA.CODFILIAL)

SELECT CODFILIAL,
       PVENDA_CORTE,
       PCT_PERIODO_CORTE,
       META_CORTE,
       DESVIO_CORTE,
       PVENDA_FALTA,
       PCT_PERIODO_FALTA,
       MEDIA_TRIM_FALTA,
       DESVIO_FALTA,
       FATURAMENTO,
       ORD,
       MOV
FROM RESULT
ORDER BY ORD, MOV DESC