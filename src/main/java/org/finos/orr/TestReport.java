package org.finos.orr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.regnosys.rosetta.common.serialisation.RosettaObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestReport {

    private static String SAMPLE = """
            {
              "originatingWorkflowStep" : {
                "businessEvent" : {
                  "intent" : "ContractFormation",
                  "eventDate" : "2021-03-09",
                  "instruction" : [ {
                    "primitiveInstruction" : {
                      "contractFormation" : {
                        "legalAgreement" : [ {
                          "legalAgreementIdentification" : {
                            "agreementName" : {
                              "agreementType" : "MasterAgreement",
                              "masterAgreementType" : {
                                "value" : "ISDAMaster",
                                "meta" : {
                                  "scheme" : "http://dtcc.com/coding-scheme/master-agreement-type"
                                }
                              }
                            },
                            "vintage" : 1992
                          },
                          "contractualParty" : [ {
                            "globalReference" : "d7edc53b",
                            "externalReference" : "PARTY2"
                          }, {
                            "globalReference" : "987391a0",
                            "externalReference" : "PARTY1"
                          } ],
                          "meta" : {
                            "globalKey" : "47759a20"
                          }
                        } ]
                      }
                    },
                    "before" : {
                      "value" : {
                        "trade" : {
                          "tradeIdentifier" : [ {
                            "issuerReference" : {
                              "globalReference" : "987391a0",
                              "externalReference" : "PARTY1"
                            },
                            "assignedIdentifier" : [ {
                              "identifier" : {
                                "value" : "DUMMYTRADEIDENTIFY01IRCCSFIXEDFIXED",
                                "meta" : {
                                  "scheme" : "http://www.fpml.org/coding-scheme/external/unique-transaction-identifier"
                                }
                              }
                            } ],
                            "meta" : {
                              "globalKey" : "f3ebf782"
                            },
                            "identifierType" : "UniqueTransactionIdentifier"
                          }, {
                            "issuerReference" : {
                              "globalReference" : "987391a0",
                              "externalReference" : "PARTY1"
                            },
                            "assignedIdentifier" : [ {
                              "identifier" : {
                                "value" : "TechnicalRecordId_380084103-2-3",
                                "meta" : {
                                  "scheme" : "TechnicalRecordId"
                                }
                              }
                            } ],
                            "meta" : {
                              "globalKey" : "77df99dd"
                            }
                          }, {
                            "issuerReference" : {
                              "globalReference" : "4fdb17f",
                              "externalReference" : "ExecutionFacility1"
                            },
                            "assignedIdentifier" : [ {
                              "identifier" : {
                                "value" : "RTN12345",
                                "meta" : {
                                  "scheme" : "RTN"
                                }
                              }
                            } ],
                            "meta" : {
                              "globalKey" : "c54c47d1"
                            }
                          } ],
                          "tradeDate" : {
                            "value" : "2021-03-09",
                            "meta" : {
                              "globalKey" : "3f28c9"
                            }
                          },
                          "tradableProduct" : {
                            "product" : {
                              "contractualProduct" : {
                                "productTaxonomy" : [ {
                                  "primaryAssetClass" : {
                                    "value" : "InterestRate"
                                  }
                                }, {
                                  "source" : "CFI",
                                  "value" : {
                                    "name" : {
                                      "value" : "SRDXCC",
                                      "meta" : {
                                        "scheme" : "http://www.fpml.org/coding-scheme/external/product-classification/iso10962"
                                      }
                                    }
                                  }
                                }, {
                                  "source" : "EMIR",
                                  "value" : {
                                    "name" : {
                                      "value" : "SW",
                                      "meta" : {
                                        "scheme" : "http://www.dtcc.com/coding-scheme/external/product-classification/emir-contract-type"
                                      }
                                    }
                                  }
                                }, {
                                  "source" : "ISDA",
                                  "productQualifier" : "InterestRate_CrossCurrency_FixedFixed"
                                } ],
                                "productIdentifier" : [ {
                                  "value" : {
                                    "identifier" : {
                                      "value" : "InterestRate:CrossCurrency:FixedFixed",
                                      "meta" : {
                                        "scheme" : "http://www.fpml.org/coding-scheme/product-taxonomy"
                                      }
                                    },
                                    "source" : "Other",
                                    "meta" : {
                                      "globalKey" : "22750897"
                                    }
                                  }
                                }, {
                                  "value" : {
                                    "identifier" : {
                                      "value" : "EZ1HPK330CY9",
                                      "meta" : {
                                        "scheme" : "http://www.fpml.org/coding-scheme/external/instrument-id-ISIN"
                                      }
                                    },
                                    "source" : "ISIN",
                                    "meta" : {
                                      "globalKey" : "da5958f5"
                                    }
                                  }
                                } ],
                                "economicTerms" : {
                                  "payout" : {
                                    "interestRatePayout" : [ {
                                      "payerReceiver" : {
                                        "payer" : "Party1",
                                        "receiver" : "Party2"
                                      },
                                      "priceQuantity" : {
                                        "quantitySchedule" : {
                                          "address" : {
                                            "scope" : "DOCUMENT",
                                            "value" : "quantity-1"
                                          }
                                        },
                                        "meta" : {
                                          "globalKey" : "0",
                                          "externalKey" : "fixedRateNotional1"
                                        }
                                      },
                                      "settlementTerms" : {
                                        "settlementCurrency" : {
                                          "value" : "USD"
                                        },
                                        "meta" : {
                                          "globalKey" : "31139414"
                                        },
                                        "cashSettlementTerms" : [ {
                                          "valuationDate" : {
                                            "fxFixingDate" : {
                                              "periodMultiplier" : 2,
                                              "period" : "D",
                                              "meta" : {
                                                "globalKey" : "2a97e7ba"
                                              },
                                              "dayType" : "Business",
                                              "businessDayConvention" : "MODFOLLOWING",
                                              "businessCenters" : {
                                                "businessCenter" : [ {
                                                  "value" : "USNY"
                                                } ],
                                                "meta" : {
                                                  "globalKey" : "27e4e9"
                                                }
                                              },
                                              "dateRelativeToPaymentDates" : {
                                                "paymentDatesReference" : [ {
                                                  "globalReference" : "3febc8e7",
                                                  "externalReference" : "paymentDates1"
                                                } ]
                                              }
                                            }
                                          },
                                          "meta" : {
                                            "globalKey" : "2a97e7ba"
                                          }
                                        } ]
                                      },
                                      "rateSpecification" : {
                                        "fixedRate" : {
                                          "rateSchedule" : {
                                            "price" : {
                                              "address" : {
                                                "scope" : "DOCUMENT",
                                                "value" : "price-1"
                                              }
                                            }
                                          },
                                          "meta" : {
                                            "globalKey" : "0"
                                          }
                                        }
                                      },
                                      "dayCountFraction" : {
                                        "value" : "ACT/365.FIXED"
                                      },
                                      "calculationPeriodDates" : {
                                        "effectiveDate" : {
                                          "adjustableDate" : {
                                            "unadjustedDate" : "2021-03-11",
                                            "meta" : {
                                              "globalKey" : "3f28cb"
                                            }
                                          },
                                          "meta" : {
                                            "globalKey" : "3f28cb"
                                          }
                                        },
                                        "terminationDate" : {
                                          "adjustableDate" : {
                                            "unadjustedDate" : "2023-05-11",
                                            "meta" : {
                                              "globalKey" : "3f394b"
                                            }
                                          },
                                          "meta" : {
                                            "globalKey" : "3f394b"
                                          }
                                        },
                                        "firstRegularPeriodStartDate" : "2021-03-11",
                                        "calculationPeriodFrequency" : {
                                          "periodMultiplier" : 6,
                                          "period" : "M",
                                          "meta" : {
                                            "globalKey" : "18a98"
                                          },
                                          "rollConvention" : "11"
                                        },
                                        "meta" : {
                                          "globalKey" : "afcc6143",
                                          "externalKey" : "fixedRateCalculationPeriodDates1"
                                        }
                                      },
                                      "paymentDates" : {
                                        "paymentFrequency" : {
                                          "periodMultiplier" : 6,
                                          "period" : "M",
                                          "meta" : {
                                            "globalKey" : "107"
                                          }
                                        },
                                        "payRelativeTo" : "CalculationPeriodEndDate",
                                        "paymentDatesAdjustments" : {
                                          "businessDayConvention" : "MODFOLLOWING",
                                          "businessCenters" : {
                                            "businessCenter" : [ {
                                              "value" : "USNY"
                                            } ],
                                            "meta" : {
                                              "globalKey" : "27e4e9"
                                            }
                                          },
                                          "meta" : {
                                            "globalKey" : "3662e8ba"
                                          }
                                        },
                                        "meta" : {
                                          "globalKey" : "3febc8e7",
                                          "externalKey" : "paymentDates1"
                                        }
                                      },
                                      "meta" : {
                                        "globalKey" : "25bb9751"
                                      }
                                    }, {
                                      "payerReceiver" : {
                                        "payer" : "Party2",
                                        "receiver" : "Party1"
                                      },
                                      "priceQuantity" : {
                                        "quantitySchedule" : {
                                          "address" : {
                                            "scope" : "DOCUMENT",
                                            "value" : "quantity-2"
                                          }
                                        },
                                        "meta" : {
                                          "globalKey" : "0",
                                          "externalKey" : "fixedRateNotional2"
                                        }
                                      },
                                      "settlementTerms" : {
                                        "settlementCurrency" : {
                                          "value" : "USD"
                                        },
                                        "meta" : {
                                          "globalKey" : "31139415"
                                        },
                                        "cashSettlementTerms" : [ {
                                          "valuationDate" : {
                                            "fxFixingDate" : {
                                              "periodMultiplier" : 2,
                                              "period" : "D",
                                              "meta" : {
                                                "globalKey" : "2a97e7bb"
                                              },
                                              "dayType" : "Business",
                                              "businessDayConvention" : "MODFOLLOWING",
                                              "businessCenters" : {
                                                "businessCenter" : [ {
                                                  "value" : "USNY"
                                                } ],
                                                "meta" : {
                                                  "globalKey" : "27e4e9"
                                                }
                                              },
                                              "dateRelativeToPaymentDates" : {
                                                "paymentDatesReference" : [ {
                                                  "globalReference" : "3febc8e7",
                                                  "externalReference" : "paymentDates2"
                                                } ]
                                              }
                                            }
                                          },
                                          "meta" : {
                                            "globalKey" : "2a97e7bb"
                                          }
                                        } ]
                                      },
                                      "rateSpecification" : {
                                        "fixedRate" : {
                                          "rateSchedule" : {
                                            "price" : {
                                              "address" : {
                                                "scope" : "DOCUMENT",
                                                "value" : "price-2"
                                              }
                                            }
                                          },
                                          "meta" : {
                                            "globalKey" : "0"
                                          }
                                        }
                                      },
                                      "dayCountFraction" : {
                                        "value" : "30/360"
                                      },
                                      "calculationPeriodDates" : {
                                        "effectiveDate" : {
                                          "adjustableDate" : {
                                            "unadjustedDate" : "2021-03-11",
                                            "meta" : {
                                              "globalKey" : "3f28cb"
                                            }
                                          },
                                          "meta" : {
                                            "globalKey" : "3f28cb"
                                          }
                                        },
                                        "terminationDate" : {
                                          "adjustableDate" : {
                                            "unadjustedDate" : "2023-05-11",
                                            "meta" : {
                                              "globalKey" : "3f394b"
                                            }
                                          },
                                          "meta" : {
                                            "globalKey" : "3f394b"
                                          }
                                        },
                                        "firstRegularPeriodStartDate" : "2021-03-11",
                                        "calculationPeriodFrequency" : {
                                          "periodMultiplier" : 6,
                                          "period" : "M",
                                          "meta" : {
                                            "globalKey" : "18a98"
                                          },
                                          "rollConvention" : "11"
                                        },
                                        "meta" : {
                                          "globalKey" : "afcc6143",
                                          "externalKey" : "fixedRateCalculationPeriodDates2"
                                        }
                                      },
                                      "paymentDates" : {
                                        "paymentFrequency" : {
                                          "periodMultiplier" : 6,
                                          "period" : "M",
                                          "meta" : {
                                            "globalKey" : "107"
                                          }
                                        },
                                        "payRelativeTo" : "CalculationPeriodEndDate",
                                        "paymentDatesAdjustments" : {
                                          "businessDayConvention" : "MODFOLLOWING",
                                          "businessCenters" : {
                                            "businessCenter" : [ {
                                              "value" : "USNY"
                                            } ],
                                            "meta" : {
                                              "globalKey" : "27e4e9"
                                            }
                                          },
                                          "meta" : {
                                            "globalKey" : "3662e8ba"
                                          }
                                        },
                                        "meta" : {
                                          "globalKey" : "3febc8e7",
                                          "externalKey" : "paymentDates2"
                                        }
                                      },
                                      "meta" : {
                                        "globalKey" : "e97e5cbe"
                                      }
                                    } ],
                                    "meta" : {
                                      "globalKey" : "30fb1d4d"
                                    }
                                  },
                                  "nonStandardisedTerms" : false
                                },
                                "meta" : {
                                  "globalKey" : "6ec3cea7"
                                }
                              },
                              "meta" : {
                                "globalKey" : "6ec3cea7"
                              }
                            },
                            "tradeLot" : [ {
                              "priceQuantity" : [ {
                                "price" : [ {
                                  "value" : {
                                    "value" : 0.12,
                                    "unit" : {
                                      "currency" : {
                                        "value" : "GBP",
                                        "meta" : {
                                          "scheme" : "http://www.fpml.org/ext/iso4217"
                                        }
                                      }
                                    },
                                    "perUnitOf" : {
                                      "currency" : {
                                        "value" : "GBP",
                                        "meta" : {
                                          "scheme" : "http://www.fpml.org/ext/iso4217"
                                        }
                                      }
                                    },
                                    "priceType" : "InterestRate"
                                  },
                                  "meta" : {
                                    "location" : [ {
                                      "scope" : "DOCUMENT",
                                      "value" : "price-1"
                                    } ]
                                  }
                                } ],
                                "quantity" : [ {
                                  "value" : {
                                    "value" : 115000000,
                                    "unit" : {
                                      "currency" : {
                                        "value" : "GBP",
                                        "meta" : {
                                          "scheme" : "http://www.fpml.org/ext/iso4217"
                                        }
                                      }
                                    }
                                  },
                                  "meta" : {
                                    "location" : [ {
                                      "scope" : "DOCUMENT",
                                      "value" : "quantity-1"
                                    } ]
                                  }
                                } ],
                                "meta" : {
                                  "globalKey" : "9f59ff16"
                                }
                              }, {
                                "price" : [ {
                                  "value" : {
                                    "value" : 0.12,
                                    "unit" : {
                                      "currency" : {
                                        "value" : "USD",
                                        "meta" : {
                                          "scheme" : "http://www.fpml.org/ext/iso4217"
                                        }
                                      }
                                    },
                                    "perUnitOf" : {
                                      "currency" : {
                                        "value" : "USD",
                                        "meta" : {
                                          "scheme" : "http://www.fpml.org/ext/iso4217"
                                        }
                                      }
                                    },
                                    "priceType" : "InterestRate"
                                  },
                                  "meta" : {
                                    "location" : [ {
                                      "scope" : "DOCUMENT",
                                      "value" : "price-2"
                                    } ]
                                  }
                                } ],
                                "quantity" : [ {
                                  "value" : {
                                    "value" : 15000000,
                                    "unit" : {
                                      "currency" : {
                                        "value" : "USD",
                                        "meta" : {
                                          "scheme" : "http://www.fpml.org/ext/iso4217"
                                        }
                                      }
                                    }
                                  },
                                  "meta" : {
                                    "location" : [ {
                                      "scope" : "DOCUMENT",
                                      "value" : "quantity-2"
                                    } ]
                                  }
                                } ],
                                "meta" : {
                                  "globalKey" : "8f57dd94"
                                }
                              } ]
                            } ],
                            "counterparty" : [ {
                              "role" : "Party1",
                              "partyReference" : {
                                "globalReference" : "d7edc53b",
                                "externalReference" : "PARTY2"
                              }
                            }, {
                              "role" : "Party2",
                              "partyReference" : {
                                "globalReference" : "987391a0",
                                "externalReference" : "PARTY1"
                              }
                            } ]
                          },
                          "party" : [ {
                            "partyId" : [ {
                              "identifier" : {
                                "value" : "987654321ZZABCDEFG11",
                                "meta" : {
                                  "scheme" : "http://www.fpml.org/coding-scheme/external/iso17442"
                                }
                              },
                              "identifierType" : "LEI",
                              "meta" : {
                                "globalKey" : "53b4dc01"
                              }
                            } ],
                            "name" : {
                              "value" : "Spain test party Belgium Branch"
                            },
                            "businessUnit" : [ {
                              "contactInformation" : {
                                "address" : [ {
                                  "country" : {
                                    "value" : "BE"
                                  }
                                } ]
                              },
                              "meta" : {
                                "globalKey" : "843"
                              }
                            } ],
                            "contactInformation" : {
                              "address" : [ {
                                "country" : {
                                  "value" : "ES"
                                }
                              } ]
                            },
                            "meta" : {
                              "globalKey" : "987391a0",
                              "externalKey" : "PARTY1"
                            }
                          }, {
                            "partyId" : [ {
                              "identifier" : {
                                "value" : "987654321ZZABCDEFG12",
                                "meta" : {
                                  "scheme" : "http://www.fpml.org/coding-scheme/external/iso17442"
                                }
                              },
                              "identifierType" : "LEI",
                              "meta" : {
                                "globalKey" : "53b4dc20"
                              }
                            } ],
                            "name" : {
                              "value" : "France test party"
                            },
                            "businessUnit" : [ {
                              "contactInformation" : {
                                "address" : [ {
                                  "country" : {
                                    "value" : "FR"
                                  }
                                } ]
                              },
                              "meta" : {
                                "globalKey" : "8cc"
                              }
                            } ],
                            "contactInformation" : {
                              "address" : [ {
                                "country" : {
                                  "value" : "FR"
                                }
                              } ]
                            },
                            "meta" : {
                              "globalKey" : "d7edc53b",
                              "externalKey" : "PARTY2"
                            }
                          }, {
                            "partyId" : [ {
                              "identifier" : {
                                "value" : "XNAS",
                                "meta" : {
                                  "scheme" : "http://www.fpml.org/coding-scheme/external/mifir/extension-iso10383"
                                }
                              },
                              "identifierType" : "MIC",
                              "meta" : {
                                "globalKey" : "4fdb17f"
                              }
                            } ],
                            "meta" : {
                              "globalKey" : "4fdb17f",
                              "externalKey" : "ExecutionFacility1"
                            }
                          } ],
                          "partyRole" : [ {
                            "partyReference" : {
                              "globalReference" : "4fdb17f",
                              "externalReference" : "ExecutionFacility1"
                            },
                            "role" : "ExecutionFacility"
                          } ],
                          "collateral" : {
                            "portfolioIdentifier" : [ {
                              "assignedIdentifier" : [ {
                                "identifier" : {
                                  "value" : "NOTAVAILABLE"
                                }
                              } ],
                              "meta" : {
                                "globalKey" : "ed264556"
                              }
                            } ],
                            "meta" : {
                              "globalKey" : "ed264556"
                            }
                          },
                          "meta" : {
                            "globalKey" : "fa2587c1"
                          }
                        },
                        "meta" : {
                          "globalKey" : "fa2587c1"
                        }
                      }
                    }
                  } ],
                  "after" : [ {
                    "trade" : {
                      "tradeIdentifier" : [ {
                        "issuerReference" : {
                          "globalReference" : "987391a0",
                          "externalReference" : "PARTY1"
                        },
                        "assignedIdentifier" : [ {
                          "identifier" : {
                            "value" : "DUMMYTRADEIDENTIFY01IRCCSFIXEDFIXED",
                            "meta" : {
                              "scheme" : "http://www.fpml.org/coding-scheme/external/unique-transaction-identifier"
                            }
                          }
                        } ],
                        "meta" : {
                          "globalKey" : "f3ebf782"
                        },
                        "identifierType" : "UniqueTransactionIdentifier"
                      }, {
                        "issuerReference" : {
                          "globalReference" : "987391a0",
                          "externalReference" : "PARTY1"
                        },
                        "assignedIdentifier" : [ {
                          "identifier" : {
                            "value" : "TechnicalRecordId_380084103-2-3",
                            "meta" : {
                              "scheme" : "TechnicalRecordId"
                            }
                          }
                        } ],
                        "meta" : {
                          "globalKey" : "77df99dd"
                        }
                      }, {
                        "issuerReference" : {
                          "globalReference" : "4fdb17f",
                          "externalReference" : "ExecutionFacility1"
                        },
                        "assignedIdentifier" : [ {
                          "identifier" : {
                            "value" : "RTN12345",
                            "meta" : {
                              "scheme" : "RTN"
                            }
                          }
                        } ],
                        "meta" : {
                          "globalKey" : "c54c47d1"
                        }
                      } ],
                      "tradeDate" : {
                        "value" : "2021-03-09",
                        "meta" : {
                          "globalKey" : "3f28c9"
                        }
                      },
                      "tradableProduct" : {
                        "product" : {
                          "contractualProduct" : {
                            "productTaxonomy" : [ {
                              "primaryAssetClass" : {
                                "value" : "InterestRate"
                              }
                            }, {
                              "source" : "CFI",
                              "value" : {
                                "name" : {
                                  "value" : "SRDXCC",
                                  "meta" : {
                                    "scheme" : "http://www.fpml.org/coding-scheme/external/product-classification/iso10962"
                                  }
                                }
                              }
                            }, {
                              "source" : "EMIR",
                              "value" : {
                                "name" : {
                                  "value" : "SW",
                                  "meta" : {
                                    "scheme" : "http://www.dtcc.com/coding-scheme/external/product-classification/emir-contract-type"
                                  }
                                }
                              }
                            }, {
                              "source" : "ISDA",
                              "productQualifier" : "InterestRate_CrossCurrency_FixedFixed"
                            } ],
                            "productIdentifier" : [ {
                              "value" : {
                                "identifier" : {
                                  "value" : "InterestRate:CrossCurrency:FixedFixed",
                                  "meta" : {
                                    "scheme" : "http://www.fpml.org/coding-scheme/product-taxonomy"
                                  }
                                },
                                "source" : "Other",
                                "meta" : {
                                  "globalKey" : "22750897"
                                }
                              }
                            }, {
                              "value" : {
                                "identifier" : {
                                  "value" : "EZ1HPK330CY9",
                                  "meta" : {
                                    "scheme" : "http://www.fpml.org/coding-scheme/external/instrument-id-ISIN"
                                  }
                                },
                                "source" : "ISIN",
                                "meta" : {
                                  "globalKey" : "da5958f5"
                                }
                              }
                            } ],
                            "economicTerms" : {
                              "payout" : {
                                "interestRatePayout" : [ {
                                  "payerReceiver" : {
                                    "payer" : "Party1",
                                    "receiver" : "Party2"
                                  },
                                  "priceQuantity" : {
                                    "quantitySchedule" : {
                                      "address" : {
                                        "scope" : "DOCUMENT",
                                        "value" : "quantity-1"
                                      }
                                    },
                                    "meta" : {
                                      "globalKey" : "0",
                                      "externalKey" : "fixedRateNotional1"
                                    }
                                  },
                                  "settlementTerms" : {
                                    "settlementCurrency" : {
                                      "value" : "USD"
                                    },
                                    "meta" : {
                                      "globalKey" : "31139414"
                                    },
                                    "cashSettlementTerms" : [ {
                                      "valuationDate" : {
                                        "fxFixingDate" : {
                                          "periodMultiplier" : 2,
                                          "period" : "D",
                                          "meta" : {
                                            "globalKey" : "2a97e7ba"
                                          },
                                          "dayType" : "Business",
                                          "businessDayConvention" : "MODFOLLOWING",
                                          "businessCenters" : {
                                            "businessCenter" : [ {
                                              "value" : "USNY"
                                            } ],
                                            "meta" : {
                                              "globalKey" : "27e4e9"
                                            }
                                          },
                                          "dateRelativeToPaymentDates" : {
                                            "paymentDatesReference" : [ {
                                              "globalReference" : "3febc8e7",
                                              "externalReference" : "paymentDates1"
                                            } ]
                                          }
                                        }
                                      },
                                      "meta" : {
                                        "globalKey" : "2a97e7ba"
                                      }
                                    } ]
                                  },
                                  "rateSpecification" : {
                                    "fixedRate" : {
                                      "rateSchedule" : {
                                        "price" : {
                                          "address" : {
                                            "scope" : "DOCUMENT",
                                            "value" : "price-1"
                                          }
                                        }
                                      },
                                      "meta" : {
                                        "globalKey" : "0"
                                      }
                                    }
                                  },
                                  "dayCountFraction" : {
                                    "value" : "ACT/365.FIXED"
                                  },
                                  "calculationPeriodDates" : {
                                    "effectiveDate" : {
                                      "adjustableDate" : {
                                        "unadjustedDate" : "2021-03-11",
                                        "meta" : {
                                          "globalKey" : "3f28cb"
                                        }
                                      },
                                      "meta" : {
                                        "globalKey" : "3f28cb"
                                      }
                                    },
                                    "terminationDate" : {
                                      "adjustableDate" : {
                                        "unadjustedDate" : "2023-05-11",
                                        "meta" : {
                                          "globalKey" : "3f394b"
                                        }
                                      },
                                      "meta" : {
                                        "globalKey" : "3f394b"
                                      }
                                    },
                                    "firstRegularPeriodStartDate" : "2021-03-11",
                                    "calculationPeriodFrequency" : {
                                      "periodMultiplier" : 6,
                                      "period" : "M",
                                      "meta" : {
                                        "globalKey" : "18a98"
                                      },
                                      "rollConvention" : "11"
                                    },
                                    "meta" : {
                                      "globalKey" : "afcc6143",
                                      "externalKey" : "fixedRateCalculationPeriodDates1"
                                    }
                                  },
                                  "paymentDates" : {
                                    "paymentFrequency" : {
                                      "periodMultiplier" : 6,
                                      "period" : "M",
                                      "meta" : {
                                        "globalKey" : "107"
                                      }
                                    },
                                    "payRelativeTo" : "CalculationPeriodEndDate",
                                    "paymentDatesAdjustments" : {
                                      "businessDayConvention" : "MODFOLLOWING",
                                      "businessCenters" : {
                                        "businessCenter" : [ {
                                          "value" : "USNY"
                                        } ],
                                        "meta" : {
                                          "globalKey" : "27e4e9"
                                        }
                                      },
                                      "meta" : {
                                        "globalKey" : "3662e8ba"
                                      }
                                    },
                                    "meta" : {
                                      "globalKey" : "3febc8e7",
                                      "externalKey" : "paymentDates1"
                                    }
                                  },
                                  "meta" : {
                                    "globalKey" : "25bb9751"
                                  }
                                }, {
                                  "payerReceiver" : {
                                    "payer" : "Party2",
                                    "receiver" : "Party1"
                                  },
                                  "priceQuantity" : {
                                    "quantitySchedule" : {
                                      "address" : {
                                        "scope" : "DOCUMENT",
                                        "value" : "quantity-2"
                                      }
                                    },
                                    "meta" : {
                                      "globalKey" : "0",
                                      "externalKey" : "fixedRateNotional2"
                                    }
                                  },
                                  "settlementTerms" : {
                                    "settlementCurrency" : {
                                      "value" : "USD"
                                    },
                                    "meta" : {
                                      "globalKey" : "31139415"
                                    },
                                    "cashSettlementTerms" : [ {
                                      "valuationDate" : {
                                        "fxFixingDate" : {
                                          "periodMultiplier" : 2,
                                          "period" : "D",
                                          "meta" : {
                                            "globalKey" : "2a97e7bb"
                                          },
                                          "dayType" : "Business",
                                          "businessDayConvention" : "MODFOLLOWING",
                                          "businessCenters" : {
                                            "businessCenter" : [ {
                                              "value" : "USNY"
                                            } ],
                                            "meta" : {
                                              "globalKey" : "27e4e9"
                                            }
                                          },
                                          "dateRelativeToPaymentDates" : {
                                            "paymentDatesReference" : [ {
                                              "globalReference" : "3febc8e7",
                                              "externalReference" : "paymentDates2"
                                            } ]
                                          }
                                        }
                                      },
                                      "meta" : {
                                        "globalKey" : "2a97e7bb"
                                      }
                                    } ]
                                  },
                                  "rateSpecification" : {
                                    "fixedRate" : {
                                      "rateSchedule" : {
                                        "price" : {
                                          "address" : {
                                            "scope" : "DOCUMENT",
                                            "value" : "price-2"
                                          }
                                        }
                                      },
                                      "meta" : {
                                        "globalKey" : "0"
                                      }
                                    }
                                  },
                                  "dayCountFraction" : {
                                    "value" : "30/360"
                                  },
                                  "calculationPeriodDates" : {
                                    "effectiveDate" : {
                                      "adjustableDate" : {
                                        "unadjustedDate" : "2021-03-11",
                                        "meta" : {
                                          "globalKey" : "3f28cb"
                                        }
                                      },
                                      "meta" : {
                                        "globalKey" : "3f28cb"
                                      }
                                    },
                                    "terminationDate" : {
                                      "adjustableDate" : {
                                        "unadjustedDate" : "2023-05-11",
                                        "meta" : {
                                          "globalKey" : "3f394b"
                                        }
                                      },
                                      "meta" : {
                                        "globalKey" : "3f394b"
                                      }
                                    },
                                    "firstRegularPeriodStartDate" : "2021-03-11",
                                    "calculationPeriodFrequency" : {
                                      "periodMultiplier" : 6,
                                      "period" : "M",
                                      "meta" : {
                                        "globalKey" : "18a98"
                                      },
                                      "rollConvention" : "11"
                                    },
                                    "meta" : {
                                      "globalKey" : "afcc6143",
                                      "externalKey" : "fixedRateCalculationPeriodDates2"
                                    }
                                  },
                                  "paymentDates" : {
                                    "paymentFrequency" : {
                                      "periodMultiplier" : 6,
                                      "period" : "M",
                                      "meta" : {
                                        "globalKey" : "107"
                                      }
                                    },
                                    "payRelativeTo" : "CalculationPeriodEndDate",
                                    "paymentDatesAdjustments" : {
                                      "businessDayConvention" : "MODFOLLOWING",
                                      "businessCenters" : {
                                        "businessCenter" : [ {
                                          "value" : "USNY"
                                        } ],
                                        "meta" : {
                                          "globalKey" : "27e4e9"
                                        }
                                      },
                                      "meta" : {
                                        "globalKey" : "3662e8ba"
                                      }
                                    },
                                    "meta" : {
                                      "globalKey" : "3febc8e7",
                                      "externalKey" : "paymentDates2"
                                    }
                                  },
                                  "meta" : {
                                    "globalKey" : "e97e5cbe"
                                  }
                                } ],
                                "meta" : {
                                  "globalKey" : "30fb1d4d"
                                }
                              },
                              "nonStandardisedTerms" : false
                            },
                            "meta" : {
                              "globalKey" : "6ec3cea7"
                            }
                          },
                          "meta" : {
                            "globalKey" : "6ec3cea7"
                          }
                        },
                        "tradeLot" : [ {
                          "priceQuantity" : [ {
                            "price" : [ {
                              "value" : {
                                "value" : 0.12,
                                "unit" : {
                                  "currency" : {
                                    "value" : "GBP",
                                    "meta" : {
                                      "scheme" : "http://www.fpml.org/ext/iso4217"
                                    }
                                  }
                                },
                                "perUnitOf" : {
                                  "currency" : {
                                    "value" : "GBP",
                                    "meta" : {
                                      "scheme" : "http://www.fpml.org/ext/iso4217"
                                    }
                                  }
                                },
                                "priceType" : "InterestRate"
                              },
                              "meta" : {
                                "location" : [ {
                                  "scope" : "DOCUMENT",
                                  "value" : "price-1"
                                } ]
                              }
                            } ],
                            "quantity" : [ {
                              "value" : {
                                "value" : 115000000,
                                "unit" : {
                                  "currency" : {
                                    "value" : "GBP",
                                    "meta" : {
                                      "scheme" : "http://www.fpml.org/ext/iso4217"
                                    }
                                  }
                                }
                              },
                              "meta" : {
                                "location" : [ {
                                  "scope" : "DOCUMENT",
                                  "value" : "quantity-1"
                                } ]
                              }
                            } ],
                            "meta" : {
                              "globalKey" : "9f59ff16"
                            }
                          }, {
                            "price" : [ {
                              "value" : {
                                "value" : 0.12,
                                "unit" : {
                                  "currency" : {
                                    "value" : "USD",
                                    "meta" : {
                                      "scheme" : "http://www.fpml.org/ext/iso4217"
                                    }
                                  }
                                },
                                "perUnitOf" : {
                                  "currency" : {
                                    "value" : "USD",
                                    "meta" : {
                                      "scheme" : "http://www.fpml.org/ext/iso4217"
                                    }
                                  }
                                },
                                "priceType" : "InterestRate"
                              },
                              "meta" : {
                                "location" : [ {
                                  "scope" : "DOCUMENT",
                                  "value" : "price-2"
                                } ]
                              }
                            } ],
                            "quantity" : [ {
                              "value" : {
                                "value" : 15000000,
                                "unit" : {
                                  "currency" : {
                                    "value" : "USD",
                                    "meta" : {
                                      "scheme" : "http://www.fpml.org/ext/iso4217"
                                    }
                                  }
                                }
                              },
                              "meta" : {
                                "location" : [ {
                                  "scope" : "DOCUMENT",
                                  "value" : "quantity-2"
                                } ]
                              }
                            } ],
                            "meta" : {
                              "globalKey" : "8f57dd94"
                            }
                          } ]
                        } ],
                        "counterparty" : [ {
                          "role" : "Party1",
                          "partyReference" : {
                            "globalReference" : "d7edc53b",
                            "externalReference" : "PARTY2"
                          }
                        }, {
                          "role" : "Party2",
                          "partyReference" : {
                            "globalReference" : "987391a0",
                            "externalReference" : "PARTY1"
                          }
                        } ]
                      },
                      "party" : [ {
                        "partyId" : [ {
                          "identifier" : {
                            "value" : "987654321ZZABCDEFG11",
                            "meta" : {
                              "scheme" : "http://www.fpml.org/coding-scheme/external/iso17442"
                            }
                          },
                          "identifierType" : "LEI",
                          "meta" : {
                            "globalKey" : "53b4dc01"
                          }
                        } ],
                        "name" : {
                          "value" : "Spain test party Belgium Branch"
                        },
                        "businessUnit" : [ {
                          "contactInformation" : {
                            "address" : [ {
                              "country" : {
                                "value" : "BE"
                              }
                            } ]
                          },
                          "meta" : {
                            "globalKey" : "843"
                          }
                        } ],
                        "contactInformation" : {
                          "address" : [ {
                            "country" : {
                              "value" : "ES"
                            }
                          } ]
                        },
                        "meta" : {
                          "globalKey" : "987391a0",
                          "externalKey" : "PARTY1"
                        }
                      }, {
                        "partyId" : [ {
                          "identifier" : {
                            "value" : "987654321ZZABCDEFG12",
                            "meta" : {
                              "scheme" : "http://www.fpml.org/coding-scheme/external/iso17442"
                            }
                          },
                          "identifierType" : "LEI",
                          "meta" : {
                            "globalKey" : "53b4dc20"
                          }
                        } ],
                        "name" : {
                          "value" : "France test party"
                        },
                        "businessUnit" : [ {
                          "contactInformation" : {
                            "address" : [ {
                              "country" : {
                                "value" : "FR"
                              }
                            } ]
                          },
                          "meta" : {
                            "globalKey" : "8cc"
                          }
                        } ],
                        "contactInformation" : {
                          "address" : [ {
                            "country" : {
                              "value" : "FR"
                            }
                          } ]
                        },
                        "meta" : {
                          "globalKey" : "d7edc53b",
                          "externalKey" : "PARTY2"
                        }
                      }, {
                        "partyId" : [ {
                          "identifier" : {
                            "value" : "XNAS",
                            "meta" : {
                              "scheme" : "http://www.fpml.org/coding-scheme/external/mifir/extension-iso10383"
                            }
                          },
                          "identifierType" : "MIC",
                          "meta" : {
                            "globalKey" : "4fdb17f"
                          }
                        } ],
                        "meta" : {
                          "globalKey" : "4fdb17f",
                          "externalKey" : "ExecutionFacility1"
                        }
                      } ],
                      "partyRole" : [ {
                        "partyReference" : {
                          "globalReference" : "4fdb17f",
                          "externalReference" : "ExecutionFacility1"
                        },
                        "role" : "ExecutionFacility"
                      } ],
                      "contractDetails" : {
                        "documentation" : [ {
                          "legalAgreementIdentification" : {
                            "agreementName" : {
                              "agreementType" : "MasterAgreement",
                              "masterAgreementType" : {
                                "value" : "ISDAMaster",
                                "meta" : {
                                  "scheme" : "http://dtcc.com/coding-scheme/master-agreement-type"
                                }
                              }
                            },
                            "vintage" : 1992
                          },
                          "contractualParty" : [ {
                            "globalReference" : "d7edc53b",
                            "externalReference" : "PARTY2"
                          }, {
                            "globalReference" : "987391a0",
                            "externalReference" : "PARTY1"
                          } ],
                          "meta" : {
                            "globalKey" : "47759a20"
                          }
                        } ]
                      },
                      "collateral" : {
                        "portfolioIdentifier" : [ {
                          "assignedIdentifier" : [ {
                            "identifier" : {
                              "value" : "NOTAVAILABLE"
                            }
                          } ],
                          "meta" : {
                            "globalKey" : "ed264556"
                          }
                        } ],
                        "meta" : {
                          "globalKey" : "ed264556"
                        }
                      },
                      "meta" : {
                        "globalKey" : "fa2587c1"
                      }
                    },
                    "state" : {
                      "positionState" : "Formed"
                    },
                    "meta" : {
                      "globalKey" : "fa2587c1"
                    }
                  } ]
                },
                "previousWorkflowStep" : {
                  "globalReference" : "21c61842"
                },
                "messageInformation" : {
                  "messageId" : {
                    "value" : "DTDApproved2380084103EMIRRiskNewNewInterest",
                    "meta" : {
                      "scheme" : "http://www.fpml.org/coding-scheme/external/technical-record-id"
                    }
                  },
                  "sentBy" : {
                    "value" : "E58DKGMJYYYJLN8C3868"
                  },
                  "sentTo" : [ {
                    "value" : "DTCCEU"
                  } ]
                },
                "timestamp" : [ {
                  "dateTime" : "2021-03-09T13:22:34Z",
                  "qualification" : "eventCreationDateTime"
                }, {
                  "dateTime" : "2021-03-09T13:22:10Z",
                  "qualification" : "executionDateTime"
                }, {
                  "dateTime" : "2021-12-10T19:19:04Z",
                  "qualification" : "confirmationDateTime"
                } ],
                "eventIdentifier" : [ {
                  "assignedIdentifier" : [ {
                    "identifier" : {
                      "value" : "DTDApproved2380084103EMIRRiskNewNewInterest"
                    }
                  } ],
                  "meta" : {
                    "globalKey" : "6dc674d6"
                  }
                } ],
                "action" : "New",
                "party" : [ {
                  "partyId" : [ {
                    "identifier" : {
                      "value" : "987654321ZZABCDEFG11",
                      "meta" : {
                        "scheme" : "http://www.fpml.org/coding-scheme/external/iso17442"
                      }
                    },
                    "identifierType" : "LEI",
                    "meta" : {
                      "globalKey" : "53b4dc01"
                    }
                  } ],
                  "name" : {
                    "value" : "Spain test party Belgium Branch"
                  },
                  "businessUnit" : [ {
                    "contactInformation" : {
                      "address" : [ {
                        "country" : {
                          "value" : "BE"
                        }
                      } ]
                    },
                    "meta" : {
                      "globalKey" : "843"
                    }
                  } ],
                  "contactInformation" : {
                    "address" : [ {
                      "country" : {
                        "value" : "ES"
                      }
                    } ]
                  },
                  "meta" : {
                    "globalKey" : "987391a0",
                    "externalKey" : "PARTY1"
                  }
                }, {
                  "partyId" : [ {
                    "identifier" : {
                      "value" : "987654321ZZABCDEFG12",
                      "meta" : {
                        "scheme" : "http://www.fpml.org/coding-scheme/external/iso17442"
                      }
                    },
                    "identifierType" : "LEI",
                    "meta" : {
                      "globalKey" : "53b4dc20"
                    }
                  } ],
                  "name" : {
                    "value" : "France test party"
                  },
                  "businessUnit" : [ {
                    "contactInformation" : {
                      "address" : [ {
                        "country" : {
                          "value" : "FR"
                        }
                      } ]
                    },
                    "meta" : {
                      "globalKey" : "8cc"
                    }
                  } ],
                  "contactInformation" : {
                    "address" : [ {
                      "country" : {
                        "value" : "FR"
                      }
                    } ]
                  },
                  "meta" : {
                    "globalKey" : "d7edc53b",
                    "externalKey" : "PARTY2"
                  }
                }, {
                  "partyId" : [ {
                    "identifier" : {
                      "value" : "XNAS",
                      "meta" : {
                        "scheme" : "http://www.fpml.org/coding-scheme/external/mifir/extension-iso10383"
                      }
                    },
                    "identifierType" : "MIC",
                    "meta" : {
                      "globalKey" : "4fdb17f"
                    }
                  } ],
                  "meta" : {
                    "globalKey" : "4fdb17f",
                    "externalKey" : "ExecutionFacility1"
                  }
                } ]
              },
              "reportableInformation" : {
                "confirmationMethod" : "NonElectronic",
                "executionVenueType" : "OffFacility",
                "intragroup" : false,
                "partyInformation" : [ {
                  "partyReference" : {
                    "globalReference" : "987391a0",
                    "externalReference" : "PARTY1"
                  },
                  "relatedParty" : [ {
                    "partyReference" : {
                      "globalReference" : "987391a0",
                      "externalReference" : "PARTY1"
                    },
                    "role" : "Beneficiary"
                  }, {
                    "partyReference" : {
                      "globalReference" : "4fdb17f",
                      "externalReference" : "ExecutionFacility1"
                    },
                    "role" : "ExecutionFacility"
                  } ],
                  "regimeInformation" : [ {
                    "regimeName" : {
                      "value" : "EMIR"
                    },
                    "supervisoryBody" : {
                      "value" : "ESMA"
                    },
                    "mandatorilyClearable" : "ProductMandatoryButNotCpty",
                    "esmaPartyInformation" : {
                      "natureOfParty" : "Financial",
                      "corporateSector" : {
                        "financialSector" : [ "INVF" ]
                      },
                      "exceedsClearingThreshold" : true
                    },
                    "reportingRole" : "ReportingParty"
                  }, {
                    "regimeName" : {
                      "value" : "UKEMIR"
                    },
                    "supervisoryBody" : {
                      "value" : "FCA"
                    },
                    "mandatorilyClearable" : "ProductMandatoryButNotCpty",
                    "fcaPartyInformation" : {
                      "natureOfParty" : "Financial",
                      "corporateSector" : {
                        "financialSector" : [ "INVF" ]
                      },
                      "exceedsClearingThreshold" : true
                    },
                    "reportingRole" : "ReportingParty"
                  }, {
                    "regimeName" : {
                      "value" : "JFSA"
                    },
                    "supervisoryBody" : {
                      "value" : "JFSA"
                    },
                    "reportingRole" : "ReportingParty",
                    "technicalRecordId" : {
                      "value" : "DTDApproved2380084103EMIRRiskNewNewInterest",
                      "meta" : {
                        "scheme" : "http://www.fpml.org/coding-scheme/external/technical-record-id"
                      }
                    }
                  } ]
                }, {
                  "partyReference" : {
                    "globalReference" : "d7edc53b",
                    "externalReference" : "PARTY2"
                  },
                  "regimeInformation" : [ {
                    "regimeName" : {
                      "value" : "EMIR"
                    },
                    "supervisoryBody" : {
                      "value" : "ESMA"
                    },
                    "mandatorilyClearable" : "ProductMandatoryButNotCpty",
                    "esmaPartyInformation" : {
                      "natureOfParty" : "Financial",
                      "corporateSector" : {
                        "financialSector" : [ "CDTI" ]
                      },
                      "exceedsClearingThreshold" : true
                    },
                    "reportingRole" : "ReportingParty"
                  }, {
                    "regimeName" : {
                      "value" : "UKEMIR"
                    },
                    "supervisoryBody" : {
                      "value" : "FCA"
                    },
                    "mandatorilyClearable" : "ProductMandatoryButNotCpty",
                    "fcaPartyInformation" : {
                      "natureOfParty" : "Financial",
                      "corporateSector" : {
                        "financialSector" : [ "CDTI" ]
                      },
                      "exceedsClearingThreshold" : true
                    },
                    "reportingRole" : "ReportingParty"
                  }, {
                    "regimeName" : {
                      "value" : "JFSA"
                    },
                    "supervisoryBody" : {
                      "value" : "JFSA"
                    },
                    "reportingRole" : "ReportingParty",
                    "technicalRecordId" : {
                      "value" : "DTDApproved2380084103EMIRRiskNewNewInterest",
                      "meta" : {
                        "scheme" : "http://www.fpml.org/coding-scheme/external/technical-record-id"
                      }
                    }
                  } ]
                } ],
                "originalExecutionTimestamp" : "2021-03-09T13:22:10Z"
              },
              "reportingSide" : {
                "reportingParty" : {
                  "globalReference" : "d7edc53b",
                  "externalReference" : "PARTY2"
                },
                "reportingCounterparty" : {
                  "globalReference" : "987391a0",
                  "externalReference" : "PARTY1"
                }
              }
            }
            """;

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        HashMap<String, String> s1 = SparkReportRunner.runReport(SAMPLE, "drr.regulation.common.TransactionReportInstruction", "drr.regulation.cftc.rewrite.reports.CFTCPart45ReportFunction");
        s1.forEach((k, v) -> System.out.println(k + " --> " + v));
    }
}
