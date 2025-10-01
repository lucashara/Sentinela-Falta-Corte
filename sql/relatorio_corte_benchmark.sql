/*-----------------------------------------------------------
   RELATÓRIO DE INDICADORES — CORTE + FATURADO (POR FILIAL)
   PERÍODO :DATAI–:DATAF

   DEFINIÇÕES
   - % CORTE (PONDERADO NO PERÍODO) = Σ(QTCORTADA*PVENDA) / Σ(VLVENDA) * 100
   - META CORTE = 0,03
   - FATURADO   = Σ(FARMAUM.VIEW_1464.VLVENDA)  (VALOR TOTAL SEM ST)

   NOTA IMPORTANTE (ORA-00904 EM ORDER BY COM UNION):
   PARA ORDENAR O CONJUNTO DO UNION ALL POR ALIAS (EX.: ORD), ENVOLVEMOS
   O UNION EM UM SUBSELECT E APLICAMOS O ORDER BY NO NÍVEL EXTERNO.
-----------------------------------------------------------*/

WITH PARAM AS (SELECT :DATAI AS DATAI, :DATAF AS DATAF
               FROM DUAL),

/* FILIAIS COM MOVIMENTO (CORTE OU FATURADO) NO PERÍODO */
     FILIAIS AS (SELECT CODFILIAL
                 FROM PCCORTEI,
                      PARAM
                 WHERE DATA BETWEEN PARAM.DATAI AND PARAM.DATAF
                 UNION
                 SELECT CODFILIAL
                 FROM FARMAUM.VIEW_1464,
                      PARAM
                 WHERE DATA BETWEEN PARAM.DATAI AND PARAM.DATAF),

/* VALORES MONETÁRIOS DE CORTE NO PERÍODO (POR FILIAL) */
     DADOS_CORTE AS (SELECT F.CODFILIAL,
                            ROUND(NVL((SELECT SUM(QTCORTADA * PVENDA)
                                       FROM PCCORTEI,
                                            PARAM
                                       WHERE DATA BETWEEN PARAM.DATAI AND PARAM.DATAF
                                         AND CODFILIAL = F.CODFILIAL), 0), 2) AS PVENDA_CORTE
                     FROM FILIAIS F),

/* FATURAMENTO (VLVENDA) NO PERÍODO (POR FILIAL) */
     DADOS_FATURAMENTO AS (SELECT F.CODFILIAL,
                                  ROUND(NVL((SELECT SUM(VLVENDA)
                                             FROM FARMAUM.VIEW_1464,
                                                  PARAM
                                             WHERE DATA BETWEEN PARAM.DATAI AND PARAM.DATAF
                                               AND CODFILIAL = F.CODFILIAL), 0), 2) AS FATURAMENTO
                           FROM FILIAIS F),

/* META FIXA DE CORTE (EM %) */
     META_CORTE AS (SELECT CODFILIAL, 0.03 AS META_CORTE
                    FROM FILIAIS),

/* POR FILIAL */
     POR_FILIAL AS (SELECT C.CODFILIAL,
                           C.PVENDA_CORTE,
                           TO_CHAR(ROUND((C.PVENDA_CORTE / NULLIF(F.FATURAMENTO, 0)) * 100, 2),
                                   'FM990D00') || '%'               AS PCT_PERIODO_CORTE,
                           TO_CHAR(T.META_CORTE, 'FM990D00') || '%' AS META_CORTE,
                           CASE
                               WHEN (C.PVENDA_CORTE / NULLIF(F.FATURAMENTO, 0)) * 100 > T.META_CORTE
                                   THEN '+' || TO_CHAR(
                                       ROUND(((C.PVENDA_CORTE / NULLIF(F.FATURAMENTO, 0)) * 100) - T.META_CORTE, 2),
                                       'FM990D00') || '% ACIMA'
                               WHEN (C.PVENDA_CORTE / NULLIF(F.FATURAMENTO, 0)) * 100 < T.META_CORTE
                                   THEN '-' || TO_CHAR(
                                       ROUND(T.META_CORTE - ((C.PVENDA_CORTE / NULLIF(F.FATURAMENTO, 0)) * 100), 2),
                                       'FM990D00') || '% ABAIXO'
                               ELSE '0% (NA META)'
                               END                                  AS DESVIO_CORTE,
                           F.FATURAMENTO,
                           0                                        AS ORD
                    FROM DADOS_CORTE C
                             JOIN DADOS_FATURAMENTO F ON F.CODFILIAL = C.CODFILIAL
                             JOIN META_CORTE T ON T.CODFILIAL = C.CODFILIAL),

/* LINHA TOTAL (PONDERADO) */
     LINHA_TOTAL AS (SELECT 'TOTAL'                                                 AS CODFILIAL,
                            SUM(C.PVENDA_CORTE)                                     AS PVENDA_CORTE,
                            TO_CHAR(ROUND((SUM(C.PVENDA_CORTE) / NULLIF(SUM(F.FATURAMENTO), 0)) * 100, 2),
                                    'FM990D00') || '%'                              AS PCT_PERIODO_CORTE,
                            TO_CHAR(ROUND(AVG(T.META_CORTE), 2), 'FM990D00') || '%' AS META_CORTE,
                            CASE
                                WHEN ROUND((SUM(C.PVENDA_CORTE) / NULLIF(SUM(F.FATURAMENTO), 0)) * 100, 2) >
                                     AVG(T.META_CORTE)
                                    THEN '+' || TO_CHAR(
                                        ROUND(((SUM(C.PVENDA_CORTE) / NULLIF(SUM(F.FATURAMENTO), 0)) * 100) -
                                              AVG(T.META_CORTE), 2),
                                        'FM990D00') || '% ACIMA'
                                WHEN ROUND((SUM(C.PVENDA_CORTE) / NULLIF(SUM(F.FATURAMENTO), 0)) * 100, 2) <
                                     AVG(T.META_CORTE)
                                    THEN '-' || TO_CHAR(
                                        ROUND(AVG(T.META_CORTE) -
                                              ((SUM(C.PVENDA_CORTE) / NULLIF(SUM(F.FATURAMENTO), 0)) * 100), 2),
                                        'FM990D00') || '% ABAIXO'
                                ELSE '0% (NA META)'
                                END                                                 AS DESVIO_CORTE,
                            SUM(F.FATURAMENTO)                                      AS FATURAMENTO,
                            1                                                       AS ORD
                     FROM DADOS_CORTE C
                              JOIN DADOS_FATURAMENTO F ON F.CODFILIAL = C.CODFILIAL
                              JOIN META_CORTE T ON T.CODFILIAL = C.CODFILIAL)

/* ======= RESULTADO FINAL ======= */
SELECT *
FROM (SELECT *
      FROM POR_FILIAL
      UNION ALL
      SELECT *
      FROM LINHA_TOTAL) R
ORDER BY R.ORD,
         CASE WHEN R.CODFILIAL = 'TOTAL' THEN 1 ELSE 0 END,
         R.CODFILIAL