package org.finos.orr;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Self-contained application to test a single Project Function locally or deployed on Spark.
 */
public class TestProject {

    private static String SAMPLE = """
            {
              "actionType" : "NEWT",
              "assetClass" : "INTR",
              "cleared" : "N",
              "clearingObligation" : "FLSE",
              "clearingThresholdOfCounterparty1" : true,
              "clearingThresholdOfCounterparty2" : true,
              "collateralPortfolioCode" : "NOTAVAILABLE",
              "collateralPortfolioIndicator" : true,
              "confirmationTimestamp" : "2021-12-10T19:19:04Z",
              "confirmed" : "YCNF",
              "contractType" : "SWAP",
              "corporateSectorOfTheCounterparty1" : [ "CDTI" ],
              "corporateSectorOfTheCounterparty2" : [ "INVF" ],
              "counterparty1" : "987654321ZZABCDEFG12",
              "counterparty2" : "987654321ZZABCDEFG11",
              "counterparty2IdentifierType" : true,
              "deliveryType" : "CASH",
              "directionOfLeg1" : "MAKE",
              "directionOfLeg2" : "TAKE",
              "effectiveDate" : "2021-03-11",
              "entityResponsibleForReporting" : "987654321ZZABCDEFG12",
              "eventDate" : "2021-03-09",
              "eventType" : "TRAD",
              "executionTimestamp" : "2021-03-09T13:22:10Z",
              "expirationDate" : "2023-05-11",
              "finalContractualSettlementDate" : "2023-05-11",
              "fixedRateDayCountConventionLeg1" : "A005",
              "fixedRateDayCountConventionLeg2" : "A001",
              "fixedRateOfLeg1" : 12.00,
              "fixedRateOfLeg2" : 12.00,
              "fixedRatePaymentFrequencyPeriodLeg1" : "MNTH",
              "fixedRatePaymentFrequencyPeriodLeg2" : "MNTH",
              "fixedRatePaymentFrequencyPeriodMultiplierLeg1" : 6,
              "fixedRatePaymentFrequencyPeriodMultiplierLeg2" : 6,
              "intragroup" : false,
              "isCrypto" : false,
              "isin" : "EZ1HPK330CY9",
              "level" : "TCTN",
              "masterAgreementType" : "ISDA",
              "masterAgreementVersion" : 1992,
              "natureOfCounterparty1" : "F",
              "natureOfCounterparty2" : "F",
              "notionalCurrency1" : "GBP",
              "notionalCurrency2" : "USD",
              "notionalLeg1" : 115000000.00000,
              "notionalLeg2" : 15000000.00000,
              "productClassification" : "SRDXCC",
              "ptrr" : false,
              "reportSubmittingEntityID" : "987654321ZZABCDEFG12",
              "reportingObligationOfTheCounterparty2" : true,
              "reportingTimestamp" : "2024-04-29T00:00:00Z",
              "settlementCurrency1" : "USD",
              "uti" : "DUMMYTRADEIDENTIFY01IRCCSFIXEDFIXED",
              "venueOfExecution" : "XNAS"
            }
            """;
    public static void main(String[] args) throws IOException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        Map<String, String> project = SparkProjectRunner.runProject(SAMPLE,
                "drr.projection.iso20022.esma.emir.refit.trade.functions.Project_EsmaEmirTradeReportToIso20022",
                "xml-config/auth030-esma-rosetta-xml-config.json");
        project.forEach((k, v) -> System.out.println(k + " --> " + v));
    }
}