import os
from google.cloud import bigquery
import pandas as pd
from datetime import date
import numpy as np

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import base64


current_dir = os.path.dirname(os.path.abspath(__file__))
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(current_dir, 'aiesec-gfb-automations-prod-df10f8050cba.json')
client = bigquery.Client()
#Get data set
sql_query = """
WITH dataset AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY entity_id,
            year,
            month,
            currency_code,
            schema_version
            ORDER BY
                recorded_at DESC
        ) as data_age
    FROM
        `aiesec-gfb-automations-prod.financial_data.executed`
    WHERE
        schema_version = "21.22" OR schema_version = "22.23"
),

Ds as
(
SELECT
entity_id,
    mapping.region as Region,
    DATE_SUB(
        DATE_ADD(DATE(year, month + 1, 1), INTERVAL 1 MONTH),
        INTERVAL 1 DAY
    ) as submitted_date,
    schema_version,
    recorded_at,
    currency_code,
    IFNULL(SAFE_CAST(__GFB__5001_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_iGV_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__5002_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_iGV_Accommodation_Fee,
    IFNULL(SAFE_CAST(__GFB__5003_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_iGV_Refunds_Returns,
    IFNULL(SAFE_CAST(__GFB__5004_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_iGV_Discounts,
    IFNULL(SAFE_CAST(__GFB__5005_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_iGV_Other,
    IFNULL(SAFE_CAST(__GFB__5006_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_oGV_Program_Fee,
    IFNULL(SAFE_CAST(__GFB__5007_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_oGV_Refunds_Returns,
    IFNULL(SAFE_CAST(__GFB__5008_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_oGV_Discounts,
    IFNULL(SAFE_CAST(__GFB__5009_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_iGTa_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__5010_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_iGTa_Refunds_Returns,
    IFNULL(SAFE_CAST(__GFB__5011_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_iGTa_Discounts,
    IFNULL(SAFE_CAST(__GFB__5012_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_iGTa_Other,
    IFNULL(SAFE_CAST(__GFB__5013_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_oGTa_Program_Fee,
    IFNULL(SAFE_CAST(__GFB__5014_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_oGTa_Refunds_Returns,
    IFNULL(SAFE_CAST(__GFB__5015_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_oGTa_Discounts,
    IFNULL(SAFE_CAST(__GFB__5016_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_iGTe_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__5017_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_iGTe_Refunds_Returns,
    IFNULL(SAFE_CAST(__GFB__5018_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_iGTe_Discounts,
    IFNULL(SAFE_CAST(__GFB__5019_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_iGTe_Other,
    IFNULL(SAFE_CAST(__GFB__5020_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_oGTe_Program_Fee,
    IFNULL(SAFE_CAST(__GFB__5021_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_oGTe_Refunds_Returns,
    IFNULL(SAFE_CAST(__GFB__5022_EX_RV_MC as float64), 0.0 ) AS MC_Direct_Revenue_oGTe_Discounts,
    IFNULL(SAFE_CAST(__GFB__5101_EA_RV_MC as float64), 0.0 ) AS MC_EwA_Revenue_YouthSpeak_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__5102_EA_RV_MC as float64), 0.0 ) AS MC_EwA_Revenue_YouthSpeak_Participant_Fee,
    IFNULL(SAFE_CAST(__GFB__5103_EA_RV_MC as float64), 0.0 ) AS MC_EwA_Revenue_YouthSpeak_Others,
    IFNULL(SAFE_CAST(__GFB__5104_EA_RV_MC as float64), 0.0 ) AS MC_EwA_Revenue_Heading_the_Future_Participant_Fee,
    IFNULL(SAFE_CAST(__GFB__5105_EA_RV_MC as float64), 0.0 ) AS MC_EwA_Revenue_Heading_the_Future_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__5106_EA_RV_MC as float64), 0.0 ) AS MC_EwA_Revenue_Heading_the_Future_Refunds_Returns,
    IFNULL(SAFE_CAST(__GFB__5107_EA_RV_MC as float64), 0.0 ) AS MC_EwA_Revenue_Heading_the_Future_Discounts,
    IFNULL(SAFE_CAST(__GFB__5108_EA_RV_MC as float64), 0.0 ) AS MC_EwA_Revenue_Heading_the_Future_Others,
    IFNULL(SAFE_CAST(__GFB__5109_EA_RV_MC as float64), 0.0 ) AS MC_EwA_Revenues_Entity_EwA_Initiatives_Participant_Fee,
    IFNULL(SAFE_CAST(__GFB__5110_EA_RV_MC as float64), 0.0 ) AS MC_EwA_Revenues_Entity_EwA_Initiatives_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__5111_EA_RV_MC as float64), 0.0 ) AS MC_EwA_Revenues_Local_Volunteer,
    IFNULL(SAFE_CAST(__GFB__5201_PT_RV_MC as float64), 0.0 ) AS MC_Piloting_Revenues_Global_Piloting_Products_Participant_Fee,
    IFNULL(SAFE_CAST(__GFB__5202_PT_RV_MC as float64), 0.0 ) AS MC_Piloting_Revenues_Global_Piloting_Products_Partners_Fee,
    IFNULL(SAFE_CAST(__GFB__5301_MG_RV_MC as float64), 0.0 ) AS MC_Project_Mgt_Revenue_Conference_and_Meetings_National_Local_Participant_Fee,
    IFNULL(SAFE_CAST(__GFB__5302_MG_RV_MC as float64), 0.0 ) AS MC_Project_Mgt_Revenue_Conference_and_Meetings_National_Local_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__5303_MG_RV_MC as float64), 0.0 ) AS MC_Project_Mgt_Revenue_Conference_and_Meetings_International_Participant_Fee,
    IFNULL(SAFE_CAST(__GFB__5304_MG_RV_MC as float64), 0.0 ) AS MC_Project_Mgt_Revenue_Conference_and_Meetings_International_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__5305_MG_RV_MC as float64), 0.0 ) AS MC_Project_Mgt_Revenue_Digital_Engagement_Participant_Fee,
    IFNULL(SAFE_CAST(__GFB__5306_MG_RV_MC as float64), 0.0 ) AS MC_Project_Mgt_Revenue_Digital_Engagement_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__5307_MG_RV_MC as float64), 0.0 ) AS MC_Project_Mgt_Revenue_Other_Portfolio_Initiatives_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__5308_MG_RV_MC as float64), 0.0 ) AS MC_Project_Mgt_Revenue_Other_Portfolio_Initiatives_Participant_Fee,
    IFNULL(SAFE_CAST(__GFB__5309_MG_RV_MC as float64), 0.0 ) AS MC_Project_Mgt_Revenue_Grants_Donations_Subsidies,
    IFNULL(SAFE_CAST(__GFB__5401_IN_RV_MC as float64), 0.0 ) AS MC_Entity_Affiliation_Fee_Revenue_iGV_Royalty,
    IFNULL(SAFE_CAST(__GFB__5402_IN_RV_MC as float64), 0.0 ) AS MC_Entity_Affiliation_Fee_Revenue_oGV_Royalty,
    IFNULL(SAFE_CAST(__GFB__5403_IN_RV_MC as float64), 0.0 ) AS MC_Entity_Affiliation_Fee_Revenue_iGTa_Royalty,
    IFNULL(SAFE_CAST(__GFB__5404_IN_RV_MC as float64), 0.0 ) AS MC_Entity_Affiliation_Fee_Revenue_oGTa_Royalty,
    IFNULL(SAFE_CAST(__GFB__5405_IN_RV_MC as float64), 0.0 ) AS MC_Entity_Affiliation_Fee_Revenue_iGTe_Royalty,
    IFNULL(SAFE_CAST(__GFB__5406_IN_RV_MC as float64), 0.0 ) AS MC_Entity_Affiliation_Fee_Revenue_oGTe_Royalty,
    IFNULL(SAFE_CAST(__GFB__5407_IN_RV_MC as float64), 0.0 ) AS MC_Entity_Affiliation_Fee_Revenue_Fixed_Payment,
    IFNULL(SAFE_CAST(__GFB__5408_IN_RV_MC as float64), 0.0 ) AS MC_Entity_Affiliation_Fee_Revenue_Other_Variable_Payment,
    IFNULL(SAFE_CAST(__GFB__5501_NE_RV_MC as float64), 0.0 ) AS MC_Miscellaneous_Revenue,
    IFNULL(SAFE_CAST(__GFB__5601_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_iGV_Marketing,
    IFNULL(SAFE_CAST(__GFB__5602_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_iGV_Accommodation,
    IFNULL(SAFE_CAST(__GFB__5603_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_iGV_Quality_Cases,
    IFNULL(SAFE_CAST(__GFB__5604_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_iGV_Other,
    IFNULL(SAFE_CAST(__GFB__5605_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_oGV_Marketing,
    IFNULL(SAFE_CAST(__GFB__5606_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_oGV_Quality_Cases,
    IFNULL(SAFE_CAST(__GFB__5607_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_oGV_Other,
    IFNULL(SAFE_CAST(__GFB__5608_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_iGTa_Marketing,
    IFNULL(SAFE_CAST(__GFB__5609_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_iGTa_Quality_Cases,
    IFNULL(SAFE_CAST(__GFB__5610_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_iGTa_Other,
    IFNULL(SAFE_CAST(__GFB__5611_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_oGTa_Marketing,
    IFNULL(SAFE_CAST(__GFB__5612_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_oGTa_Quality_Cases,
    IFNULL(SAFE_CAST(__GFB__5613_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_oGTa_Other,
    IFNULL(SAFE_CAST(__GFB__5614_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_iGTe_Marketing,
    IFNULL(SAFE_CAST(__GFB__5615_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_iGTe_Quality_Cases,
    IFNULL(SAFE_CAST(__GFB__5616_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_iGTe_Other,
    IFNULL(SAFE_CAST(__GFB__5617_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_oGTe_Marketing,
    IFNULL(SAFE_CAST(__GFB__5618_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_oGTe_Quality_Cases,
    IFNULL(SAFE_CAST(__GFB__5619_EX_CO_MC as float64), 0.0 ) AS MC_Direct_Costs_oGTe_Other,
    IFNULL(SAFE_CAST(__GFB__5701_EA_CO_MC as float64), 0.0 ) AS MC_EwA_Costs_YouthSpeak,
    IFNULL(SAFE_CAST(__GFB__5702_EA_CO_MC as float64), 0.0 ) AS MC_EwA_Costs_YouthSpeak_Marketing,
    IFNULL(SAFE_CAST(__GFB__5703_EA_CO_MC as float64), 0.0 ) AS MC_EwA_Cost_Heading_for_the_Future,
    IFNULL(SAFE_CAST(__GFB__5704_EA_CO_MC as float64), 0.0 ) AS MC_EwA_Cost_Heading_for_the_Future_Marketing,
    IFNULL(SAFE_CAST(__GFB__5705_EA_CO_MC as float64), 0.0 ) AS MC_EwA_Cost_Entity_EwA_Initiatives,
    IFNULL(SAFE_CAST(__GFB__5706_EA_CO_MC as float64), 0.0 ) AS MC_EwA_Costs_Local_Volunteer,
    IFNULL(SAFE_CAST(__GFB__5801_PT_CO_MC as float64), 0.0 ) AS MC_Piloting_Costs_Global_Piloting_Products,
    IFNULL(SAFE_CAST(__GFB__5901_MG_CO_MC as float64), 0.0 ) AS MC_Project_Mgt_Costs_Conference_and_Meetings_National_Local,
    IFNULL(SAFE_CAST(__GFB__5902_MG_CO_MC as float64), 0.0 ) AS MC_Project_Mgt_Costs_Conference_and_Meetings_International,
    IFNULL(SAFE_CAST(__GFB__5903_MG_CO_MC as float64), 0.0 ) AS MC_Project_Mgt_Costs_Digital_Engagement,
    IFNULL(SAFE_CAST(__GFB__5904_MG_CO_MC as float64), 0.0 ) AS MC_Project_Mgt_Costs_Other_Portfolio_and_Initiatives,
    IFNULL(SAFE_CAST(__GFB__5905_MG_CO_MC as float64), 0.0 ) AS MC_Project_Mgt_Costs_Partnership_Logistics,
    IFNULL(SAFE_CAST(__GFB__6001_FN_CO_MC as float64), 0.0 ) AS MC_Bank_Fees,
    IFNULL(SAFE_CAST(__GFB__6002_FN_CO_MC as float64), 0.0 ) AS MC_Taxes,
    IFNULL(SAFE_CAST(__GFB__6101_OH_CO_MC as float64), 0.0 ) AS MC_Overhead_Costs_Office,
    IFNULL(SAFE_CAST(__GFB__6102_OH_CO_MC as float64), 0.0 ) AS MC_Overhead_Costs_HR,
    IFNULL(SAFE_CAST(__GFB__6103_OH_CO_MC as float64), 0.0 ) AS MC_Overhead_Costs_Legality,
    IFNULL(SAFE_CAST(__GFB__6104_OH_CO_MC as float64), 0.0 ) AS MC_Overhead_Costs_Planning_LnD,
    IFNULL(SAFE_CAST(__GFB__6105_OH_CO_MC as float64), 0.0 ) AS MC_Overhead_Costs_Other_Marketing,
    IFNULL(SAFE_CAST(__GFB__6106_OH_CO_MC as float64), 0.0 ) AS MC_Overhead_Costs_PR_Branding_Costs,
    IFNULL(SAFE_CAST(__GFB__6107_OH_CO_MC as float64), 0.0 ) AS MC_Overhead_Costs_Entity_Coaching_Visits,
    IFNULL(SAFE_CAST(__GFB__6108_OH_CO_MC as float64), 0.0 ) AS MC_Overhead_Costs_National_Conf_Travelling,
    IFNULL(SAFE_CAST(__GFB__6109_OH_CO_MC as float64), 0.0 ) AS MC_Overhead_Costs_International_Conf_Travelling_Visa,
    IFNULL(SAFE_CAST(__GFB__6110_OH_CO_MC as float64), 0.0 ) AS MC_Overhead_Costs_Platforms,
    IFNULL(SAFE_CAST(__GFB__6301_GL_CO_MC as float64), 0.0 ) AS MC_Global_Affiliation_Fee_Costs_AI_RO_Fees,
    IFNULL(SAFE_CAST(__GFB__6401_NE_CO_MC as float64), 0.0 ) AS MC_Uncollectible_Accounts_Costs,
    IFNULL(SAFE_CAST(__GFB__6402_NE_CO_MC as float64), 0.0 ) AS MC_Miscellaneous_Costs,
    IFNULL(SAFE_CAST(__GFB__7001_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_iGV_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__7002_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_iGV_Accommodation_Fee,
    IFNULL(SAFE_CAST(__GFB__7003_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_iGV_Refunds_Returns_contra_account,
    IFNULL(SAFE_CAST(__GFB__7004_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_iGV_Discounts_contra_account,
    IFNULL(SAFE_CAST(__GFB__7005_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_iGV_Other,
    IFNULL(SAFE_CAST(__GFB__7006_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_oGV_Program_Fee,
    IFNULL(SAFE_CAST(__GFB__7007_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_oGV_Refunds_Returns_contra_account,
    IFNULL(SAFE_CAST(__GFB__7008_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_oGV_Discounts_contra_account,
    IFNULL(SAFE_CAST(__GFB__7009_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_iGTa_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__7010_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_iGTa_Refunds_Returns_contra_account,
    IFNULL(SAFE_CAST(__GFB__7011_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_iGTa_Discounts_contra_account,
    IFNULL(SAFE_CAST(__GFB__7012_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_iGTa_Other,
    IFNULL(SAFE_CAST(__GFB__7013_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_oGTa_Program_Fee,
    IFNULL(SAFE_CAST(__GFB__7014_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_oGTa_Refunds_Returns_contra_account,
    IFNULL(SAFE_CAST(__GFB__7015_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_oGTa_Discounts_contra_account,
    IFNULL(SAFE_CAST(__GFB__7016_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_iGTe_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__7017_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_iGTe_Refunds_Returns_contra_account,
    IFNULL(SAFE_CAST(__GFB__7018_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_iGTe_Discounts_contra_account,
    IFNULL(SAFE_CAST(__GFB__7019_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_iGTe_Other,
    IFNULL(SAFE_CAST(__GFB__7020_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_oGTe_Program_Fee,
    IFNULL(SAFE_CAST(__GFB__7021_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_oGTe_Refunds_Returns_contra_account,
    IFNULL(SAFE_CAST(__GFB__7022_EX_RV_LC as float64), 0.0 ) AS LC_Direct_Revenue_oGTe_Discounts_contra_account,
    IFNULL(SAFE_CAST(__GFB__7101_EA_RV_LC as float64), 0.0 ) AS LC_EwA_Revenue_YouthSpeak_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__7102_EA_RV_LC as float64), 0.0 ) AS LC_EwA_Revenue_YouthSpeak_Participant_Fee,
    IFNULL(SAFE_CAST(__GFB__7103_EA_RV_LC as float64), 0.0 ) AS LC_EwA_Revenue_YouthSpeak_Others,
    IFNULL(SAFE_CAST(__GFB__7104_EA_RV_LC as float64), 0.0 ) AS LC_EwA_Revenue_Heading_the_Future_Participant_Fee,
    IFNULL(SAFE_CAST(__GFB__7105_EA_RV_LC as float64), 0.0 ) AS LC_EwA_Revenue_Heading_the_Future_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__7106_EA_RV_LC as float64), 0.0 ) AS LC_EwA_Revenue_Heading_the_Future_Refunds_Returns_contra_account,
    IFNULL(SAFE_CAST(__GFB__7107_EA_RV_LC as float64), 0.0 ) AS LC_EwA_Revenue_Heading_the_Future_Discounts_contra_account,
    IFNULL(SAFE_CAST(__GFB__7108_EA_RV_LC as float64), 0.0 ) AS LC_EwA_Revenue_Heading_the_Future_Others,
    IFNULL(SAFE_CAST(__GFB__7109_EA_RV_LC as float64), 0.0 ) AS LC_EwA_Revenues_Entity_EwA_Initiatives_Participant_Fee,
    IFNULL(SAFE_CAST(__GFB__7110_EA_RV_LC as float64), 0.0 ) AS LC_EwA_Revenues_Entity_EwA_Initiatives_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__7111_EA_RV_LC as float64), 0.0 ) AS LC_EwA_Revenues_Local_Volunteer,
    IFNULL(SAFE_CAST(__GFB__7301_MG_RV_LC as float64), 0.0 ) AS LC_Project_Mgt_Revenue_Conference_and_Meetings_National_Local_Participant_Fee,
    IFNULL(SAFE_CAST(__GFB__7302_MG_RV_LC as float64), 0.0 ) AS LC_Project_Mgt_Revenue_Conference_and_Meetings_National_Local_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__7303_MG_RV_LC as float64), 0.0 ) AS LC_Project_Mgt_Revenue_Conference_and_Meetings_International_Participant_Fee,
    IFNULL(SAFE_CAST(__GFB__7304_MG_RV_LC as float64), 0.0 ) AS LC_Project_Mgt_Revenue_Conference_and_Meetings_International_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__7305_MG_RV_LC as float64), 0.0 ) AS LC_Project_Mgt_Revenue_Digital_Engagement_Participant_Fee,
    IFNULL(SAFE_CAST(__GFB__7306_MG_RV_LC as float64), 0.0 ) AS LC_Project_Mgt_Revenue_Digital_Engagement_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__7307_MG_RV_LC as float64), 0.0 ) AS LC_Project_Mgt_Revenue_Other_Portfolio_Initiatives_Partner_Fee,
    IFNULL(SAFE_CAST(__GFB__7308_MG_RV_LC as float64), 0.0 ) AS LC_Project_Mgt_Revenue_Other_Portfolio_Initiatives_Participant_Fee,
    IFNULL(SAFE_CAST(__GFB__7309_MG_RV_LC as float64), 0.0 ) AS LC_Project_Mgt_Revenue_Grants_Donations_Subsidies,
    IFNULL(SAFE_CAST(__GFB__7501_NE_RV_LC as float64), 0.0 ) AS LC_Miscellaneous_Revenue,
    IFNULL(SAFE_CAST(__GFB__7601_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_iGV_Marketing,
    IFNULL(SAFE_CAST(__GFB__7602_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_iGV_Accommodation,
    IFNULL(SAFE_CAST(__GFB__7603_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_iGV_Quality_Cases,
    IFNULL(SAFE_CAST(__GFB__7604_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_iGV_Other,
    IFNULL(SAFE_CAST(__GFB__7605_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_oGV_Marketing,
    IFNULL(SAFE_CAST(__GFB__7606_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_oGV_Quality_Cases,
    IFNULL(SAFE_CAST(__GFB__7607_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_oGV_Other,
    IFNULL(SAFE_CAST(__GFB__7608_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_iGTa_Marketing,
    IFNULL(SAFE_CAST(__GFB__7609_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_iGTa_Quality_Cases,
    IFNULL(SAFE_CAST(__GFB__7610_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_iGTa_Other,
    IFNULL(SAFE_CAST(__GFB__7611_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_oGTa_Marketing,
    IFNULL(SAFE_CAST(__GFB__7612_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_oGTa_Quality_Cases,
    IFNULL(SAFE_CAST(__GFB__7613_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_oGTa_Other,
    IFNULL(SAFE_CAST(__GFB__7614_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_iGTe_Marketing,
    IFNULL(SAFE_CAST(__GFB__7615_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_iGTe_Quality_Cases,
    IFNULL(SAFE_CAST(__GFB__7616_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_iGTe_Other,
    IFNULL(SAFE_CAST(__GFB__7617_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_oGTe_Marketing,
    IFNULL(SAFE_CAST(__GFB__7618_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_oGTe_Quality_Cases,
    IFNULL(SAFE_CAST(__GFB__7619_EX_CO_LC as float64), 0.0 ) AS LC_Direct_Costs_oGTe_Other,
    IFNULL(SAFE_CAST(__GFB__7701_EA_CO_LC as float64), 0.0 ) AS LC_EwA_Costs_YouthSpeak,
    IFNULL(SAFE_CAST(__GFB__7702_EA_CO_LC as float64), 0.0 ) AS LC_EwA_Costs_YouthSpeak_Marketing,
    IFNULL(SAFE_CAST(__GFB__7703_EA_CO_LC as float64), 0.0 ) AS LC_EwA_Cost_Heading_for_the_Future,
    IFNULL(SAFE_CAST(__GFB__7704_EA_CO_LC as float64), 0.0 ) AS LC_EwA_Cost_Heading_for_the_Future_Marketing,
    IFNULL(SAFE_CAST(__GFB__7705_EA_CO_LC as float64), 0.0 ) AS LC_EwA_Cost_Entity_EwA_Initiatives,
    IFNULL(SAFE_CAST(__GFB__7706_EA_CO_LC as float64), 0.0 ) AS LC_EwA_Costs_Local_Volunteer,
    IFNULL(SAFE_CAST(__GFB__7901_MG_CO_LC as float64), 0.0 ) AS LC_Project_Mgt_Costs_Conference_and_Meetings_National_Local,
    IFNULL(SAFE_CAST(__GFB__7902_MG_CO_LC as float64), 0.0 ) AS LC_Project_Mgt_Costs_Conference_and_Meetings_International,
    IFNULL(SAFE_CAST(__GFB__7903_MG_CO_LC as float64), 0.0 ) AS LC_Project_Mgt_Costs_Digital_Engagement,
    IFNULL(SAFE_CAST(__GFB__7904_MG_CO_LC as float64), 0.0 ) AS LC_Project_Mgt_Costs_Other_Portfolio_Initiatives,
    IFNULL(SAFE_CAST(__GFB__7905_MG_CO_LC as float64), 0.0 ) AS LC_Project_Mgt_Costs_Partnership_Logistics,
    IFNULL(SAFE_CAST(__GFB__8001_FN_CO_LC as float64), 0.0 ) AS LC_Bank_Fees,
    IFNULL(SAFE_CAST(__GFB__8002_FN_CO_LC as float64), 0.0 ) AS LC_Taxes,
    IFNULL(SAFE_CAST(__GFB__8101_OH_CO_LC as float64), 0.0 ) AS LC_Overhead_Costs_Office,
    IFNULL(SAFE_CAST(__GFB__8102_OH_CO_LC as float64), 0.0 ) AS LC_Overhead_Costs_HR,
    IFNULL(SAFE_CAST(__GFB__8103_OH_CO_LC as float64), 0.0 ) AS LC_Overhead_Costs_Legality,
    IFNULL(SAFE_CAST(__GFB__8104_OH_CO_LC as float64), 0.0 ) AS LC_Overhead_Costs_Planning_LnD,
    IFNULL(SAFE_CAST(__GFB__8105_OH_CO_LC as float64), 0.0 ) AS LC_Overhead_Costs_Other_Marketing,
    IFNULL(SAFE_CAST(__GFB__8106_OH_CO_LC as float64), 0.0 ) AS LC_Overhead_Costs_PR_Branding_Costs,
    IFNULL(SAFE_CAST(__GFB__8108_OH_CO_LC as float64), 0.0 ) AS LC_Overhead_Costs_National_Conf_Travelling,
    IFNULL(SAFE_CAST(__GFB__8109_OH_CO_LC as float64), 0.0 ) AS LC_Overhead_Costs_International_Conf_Travelling_Visa,
    IFNULL(SAFE_CAST(__GFB__8201_IN_CO_LC as float64), 0.0 ) AS LC_Entity_Affiliation_Fee_Costs_iGV_Royalty,
    IFNULL(SAFE_CAST(__GFB__8202_IN_CO_LC as float64), 0.0 ) AS LC_Entity_Affiliation_Fee_Costs_oGV_Royalty,
    IFNULL(SAFE_CAST(__GFB__8203_IN_CO_LC as float64), 0.0 ) AS LC_Entity_Affiliation_Fee_Costs_iGTa_Royalty,
    IFNULL(SAFE_CAST(__GFB__8204_IN_CO_LC as float64), 0.0 ) AS LC_Entity_Affiliation_Fee_Costs_oGTa_Royalty,
    IFNULL(SAFE_CAST(__GFB__8205_IN_CO_LC as float64), 0.0 ) AS LC_Entity_Affiliation_Fee_Costs_iGTe_Royalty,
    IFNULL(SAFE_CAST(__GFB__8206_IN_CO_LC as float64), 0.0 ) AS LC_Entity_Affiliation_Fee_Costs_oGTe_Royalty,
    IFNULL(SAFE_CAST(__GFB__8207_IN_CO_LC as float64), 0.0 ) AS LC_Entity_Affiliation_Fee_Costs_Fixed_Payment,
    IFNULL(SAFE_CAST(__GFB__8208_IN_CO_LC as float64), 0.0 ) AS LC_Entity_Affiliation_Fee_Costs_Other_Variable_Payment,
    IFNULL(SAFE_CAST(__GFB__8401_NE_CO_LC as float64), 0.0 ) AS LC_Uncollectible_Accounts_Costs,
    IFNULL(SAFE_CAST(__GFB__8402_NE_CO_LC as float64), 0.0 ) AS LC_Miscellaneous_Costs,
    IFNULL(SAFE_CAST(__GFB__6501_CA_AS_MC as float64), 0.00) AS MC_Bank_Account,
    IFNULL(SAFE_CAST(__GFB__6502_CA_AS_MC as float64), 0.00) AS MC_Petty_Cash,
    IFNULL(SAFE_CAST(__GFB__6601_LA_AS_MC as float64), 0.00) AS MC_Long_Term_Assets_Property,
    IFNULL(SAFE_CAST(__GFB__6602_LA_AS_MC as float64), 0.00) AS MC_Long_Term_Assets_Reserves,
    IFNULL(SAFE_CAST(__GFB__6603_LA_AS_MC as float64), 0.00) AS MC_Long_Term_Assets_Financial_Property,
    IFNULL(SAFE_CAST(__GFB__6604_LA_AS_MC as float64), 0.00) AS MC_Long_Term_Assets_Other,
    IFNULL(SAFE_CAST(__GFB__6605_IN_AS_MC as float64), 0.00) AS MC_Long_Term_Receivables_Internal_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__6606_LR_AS_MC as float64), 0.00) AS MC_Long_Term_Receivables_External_Partners,
    IFNULL(SAFE_CAST(__GFB__6607_LR_AS_MC as float64), 0.00) AS MC_Long_Term_Receivables_External_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__6608_LR_AS_MC as float64), 0.00) AS MC_Long_Term_Receivables_External_AI_RO,
    IFNULL(SAFE_CAST(__GFB__6609_SR_AS_MC as float64), 0.00) AS MC_Prepaid_Expenses,
    IFNULL(SAFE_CAST(__GFB__6610_IN_AS_MC as float64), 0.00) AS MC_Short_Term_Receivables_Internal_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__6611_SR_AS_MC as float64), 0.00) AS MC_Short_Term_Receivables_External_Members,
    IFNULL(SAFE_CAST(__GFB__6612_SR_AS_MC as float64), 0.00) AS MC_Short_Term_Receivables_External_Youth,
    IFNULL(SAFE_CAST(__GFB__6613_SR_AS_MC as float64), 0.00) AS MC_Short_Term_Receivables_External_Partners,
    IFNULL(SAFE_CAST(__GFB__6614_SR_AS_MC as float64), 0.00) AS MC_Short_Term_Receivables_External_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__6615_SR_AS_MC as float64), 0.00) AS MC_Short_Term_Receivables_External_AI_RO,
    IFNULL(SAFE_CAST(__GFB__6616_SR_AS_MC as float64), 0.00) AS MC_Short_Term_Receivables_iGTa_Partners_Salaries_Accommodation_Transfer,
    IFNULL(SAFE_CAST(__GFB__6617_SR_AS_MC as float64), 0.00) AS MC_Short_Term_Receivables_iGTe_Partners_Salaries_Transfer,
    IFNULL(SAFE_CAST(__GFB__6618_SR_AS_MC as float64), 0.00) AS MC_Allowance_for_Uncollectible_Accounts_contra_account,
    IFNULL(SAFE_CAST(__GFB__6701_EQ_LE_MC as float64), 0.00) AS MC_Equity,
    IFNULL(SAFE_CAST(__GFB__6801_IN_LE_MC as float64), 0.00) AS MC_Long_Term_Liabilities_Internal_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__6802_LL_LE_MC as float64), 0.00) AS MC_Long_Term_Liabilities_External_Partners,
    IFNULL(SAFE_CAST(__GFB__6803_LL_LE_MC as float64), 0.00) AS MC_Long_Term_Liabilities_External_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__6804_LL_LE_MC as float64), 0.00) AS MC_Long_Term_Liabilities_External_AI_RO,
    IFNULL(SAFE_CAST(__GFB__6805_LL_LE_MC as float64), 0.00) AS MC_Long_Term_Liabilities_External_Other_Externals,
    IFNULL(SAFE_CAST(__GFB__6806_SL_LE_MC as float64), 0.00) AS MC_Prepaid_Incomes,
    IFNULL(SAFE_CAST(__GFB__6807_IN_LE_MC as float64), 0.00) AS MC_Short_Term_Liabilities_Internal_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__6808_SL_LE_MC as float64), 0.00) AS MC_Short_Term_Liabilities_External_Members,
    IFNULL(SAFE_CAST(__GFB__6809_SL_LE_MC as float64), 0.00) AS MC_Short_Term_Liabilities_External_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__6810_SL_LE_MC as float64), 0.00) AS MC_Short_Term_Liabilities_External_AI_RO,
    IFNULL(SAFE_CAST(__GFB__6811_SL_LE_MC as float64), 0.00) AS MC_Short_Term_Liabilities_External_Other_Externals,
    IFNULL(SAFE_CAST(__GFB__6812_SL_LE_MC as float64), 0.00) AS MC_Short_Term_Liabilities_iGTa_Exchange_Participants_Salaries_Accommodation_Transfer,
    IFNULL(SAFE_CAST(__GFB__6813_SL_LE_MC as float64), 0.00) AS MC_Short_Term_Liabilities_iGTe_Exchange_Participants_Salaries_Transfer,
    IFNULL(SAFE_CAST(__GFB__8501_CA_AS_LC as float64), 0.00) AS LC_Bank_Account,
    IFNULL(SAFE_CAST(__GFB__8502_CA_AS_LC as float64), 0.00) AS LC_Petty_Cash,
    IFNULL(SAFE_CAST(__GFB__8601_LA_AS_LC as float64), 0.00) AS LC_Long_Term_Assets_Property,
    IFNULL(SAFE_CAST(__GFB__8602_LA_AS_LC as float64), 0.00) AS LC_Long_Term_Assets_Reserves,
    IFNULL(SAFE_CAST(__GFB__8603_LA_AS_LC as float64), 0.00) AS LC_Long_Term_Assets_Financial_Property,
    IFNULL(SAFE_CAST(__GFB__8604_LA_AS_LC as float64), 0.00) AS LC_Long_Term_Assets_Other,
    IFNULL(SAFE_CAST(__GFB__8605_IN_AS_LC as float64), 0.00) AS LC_Long_Term_Receivables_Internal_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__8606_LR_AS_LC as float64), 0.00) AS LC_Long_Term_Receivables_External_Partners,
    IFNULL(SAFE_CAST(__GFB__8607_LR_AS_LC as float64), 0.00) AS LC_Long_Term_Receivables_External_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__8609_SR_AS_LC as float64), 0.00) AS LC_Prepaid_Expenses,
    IFNULL(SAFE_CAST(__GFB__8611_SR_AS_LC as float64), 0.00) AS LC_Short_Term_Receivables_Internal_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__8612_SR_AS_LC as float64), 0.00) AS LC_Short_Term_Receivables_External_Members,
    IFNULL(SAFE_CAST(__GFB__8613_SR_AS_LC as float64), 0.00) AS LC_Short_Term_Receivables_External_Youth,
    IFNULL(SAFE_CAST(__GFB__8614_SR_AS_LC as float64), 0.00) AS LC_Short_Term_Receivables_External_Partners,
    IFNULL(SAFE_CAST(__GFB__8615_SR_AS_LC as float64), 0.00) AS LC_Short_Term_Receivables_External_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__8616_SR_AS_LC as float64), 0.00) AS LC_Short_Term_Receivables_iGTa_Partners_Salaries_Accommodation_Transfer,
    IFNULL(SAFE_CAST(__GFB__8617_SR_AS_LC as float64), 0.00) AS LC_Short_Term_Receivables_iGTe_Partners_Salaries_Transfer,
    IFNULL(SAFE_CAST(__GFB__8618_SR_AS_LC as float64), 0.00) AS LC_Allowance_for_Uncollectible_Accounts_contra_account,
    IFNULL(SAFE_CAST(__GFB__8701_EQ_LE_LC as float64), 0.00) AS LC_Equity,
    IFNULL(SAFE_CAST(__GFB__8801_IN_LE_LC as float64), 0.00) AS LC_Long_Term_Liabilities_Internal_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__8802_LL_LE_LC as float64), 0.00) AS LC_Long_Term_Liabilities_External_Partners,
    IFNULL(SAFE_CAST(__GFB__8803_LL_LE_LC as float64), 0.00) AS LC_Long_Term_Liabilities_External_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__8805_LL_LE_LC as float64), 0.00) AS LC_Long_Term_Liabilities_External_Other_Externals,
    IFNULL(SAFE_CAST(__GFB__8806_SL_LE_LC as float64), 0.00) AS LC_Prepaid_Incomes,
    IFNULL(SAFE_CAST(__GFB__8807_IN_LE_LC as float64), 0.00) AS LC_Short_Term_Liabilities_Internal_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__8808_SL_LE_LC as float64), 0.00) AS LC_Short_Term_Liabilities_External_Members,
    IFNULL(SAFE_CAST(__GFB__8809_SL_LE_LC as float64), 0.00) AS LC_Short_Term_Liabilities_External_AIESEC_Entities,
    IFNULL(SAFE_CAST(__GFB__8811_SL_LE_LC as float64), 0.00) AS LC_Short_Term_Liabilities_External_Other_Externals,
    IFNULL(SAFE_CAST(__GFB__8812_SL_LE_LC as float64), 0.00) AS LC_Short_Term_Liabilities_iGTa_Exchange_Participants_Salaries_Accommodation_Transfer,
    IFNULL(SAFE_CAST(__GFB__8813_SL_LE_LC as float64), 0.00) AS LC_Short_Term_Liabilities_iGTe_Exchange_Participants_Salaries_Transfer
FROM
    dataset
    INNER JOIN `aiesec-gfb-automations-prod.financial_data.iso_region_mapping` as mapping ON (mapping.iso = entity_id)
WHERE
    data_age = 1

)

SELECT *
FROM Ds;
"""

query_job = client.query(sql_query)




#  create data frame

df = pd.DataFrame()

df = query_job.to_dataframe()

df['submitted_date'] = df['submitted_date'].astype('datetime64[M]')

df = df.sort_values(by='submitted_date').reset_index()
df = df.drop('index', axis=1)



# Calculation Outlier Detection Objects
#Cash and cash equivalents
df['Bank Account'] = df['MC_Bank_Account']
df['Petty Cash'] = df['MC_Petty_Cash']
df['Long Term Assets (Reserves)'] = df['MC_Long_Term_Assets_Reserves']
df['Long Term Assets (Financial Property)'] = df['MC_Long_Term_Assets_Financial_Property']



#Accounts receivable
df['Long Term Receivables: External (Partners)'] = df['MC_Long_Term_Receivables_External_Partners']
df['Short Term Receivables: External (Members)'] = df['MC_Short_Term_Receivables_External_Members']
df['Short Term Receivables: External (Youth)'] = df['MC_Short_Term_Receivables_External_Youth']
df['Short Term Receivables: External (Partners)'] = df['MC_Short_Term_Receivables_External_Partners']



#Fixed asset
df['Long Term Assets (Property)'] = df['MC_Long_Term_Assets_Property']
df['Long Term Assets (Other)'] = df['MC_Long_Term_Assets_Other']



#Accounts payable
df['Long Term Liabilities: External (Partners)'] = df['MC_Long_Term_Liabilities_External_Partners']
df['Long Term Liabilities: External (Other Externals)'] = df['MC_Long_Term_Liabilities_External_Other_Externals']
df['Short Term Liabilities: External (Members)'] = df['MC_Short_Term_Liabilities_External_Members']
df['Short Term Liabilities: External (Other Externals)'] = df['MC_Short_Term_Liabilities_External_Other_Externals']


# Note df = df[['column_1','column_2',column_3',...]]


#Accruals and deferred charges
df['Prepaid Expenses'] = df['MC_Prepaid_Expenses']
df['Allowance for Uncollectible Accounts (contra account)'] = df['MC_Allowance_for_Uncollectible_Accounts_contra_account']
df['Prepaid Incomes'] = df['MC_Prepaid_Incomes']



#Revenue and expenses
df['Miscellaneous Revenue'] = df['MC_Miscellaneous_Revenue']
df['Uncollectible Accounts Costs'] = df['MC_Uncollectible_Accounts_Costs']
df['Miscellaneous Costs'] = df['MC_Miscellaneous_Costs']



#Payroll and employee benefits
df['Overhead Costs: (HR)'] = df['MC_Overhead_Costs_HR']




# Define Lastest Date

today = date.today()

mm = str(today.month)
mm = "0"+mm if len(mm) == 1 else mm

# dd="0"+dd if len(dd) == 1 else dd
dd= "01"

td = str(today.year) + "-" + mm + "-" + dd




# Define Argument

df = df.copy()
currency_code = "CAD"
start_date = "2022-06-01"
end_date = td



# Define a list of entity IDs to check
# entity_ids = df.entity_id.unique()
entity_ids = ['ARG', 'BOL', 'BRA', 'CHL', 'COL', 'ECU', 'GTM', 'MEX', 'NIC', 'PAN', 'USA', 'VEN', 'AUS', 'BGD', 'HKG', 'IDN', 'IND', 'JPN', 'KOR', 'LKA', 'MYS', 'PAK', 'PHL', 'THA', 'TWN', 'VNM', 'ALB', 'ARM', 'AUT', 'AZE', 'BEL', 'BGR', 'BIH', 'CHE', 'CZE', 'DEU', 'DNK', 'ESP', 'FIN', 'GBR', 'GEO', 'GRC', 'HUN', 'ISL', 'ITA', 'LTU', 'NLD', 'NOR', 'POL', 'PRT', 'ROU', 'RUS', 'SRB', 'SVK', 'SWE', 'TUR', 'UKR', 'ARE', 'BEN', 'BFA', 'CIV', 'EGY', 'GHA', 'JOR', 'LBN', 'LBR', 'MAR', 'NGA', 'RWA', 'SEN', 'TGO', 'TUN', 'HRV', 'MMR', 'TZA']

# Define a list of accounts to check
check_accounts =['Bank Account', 'Petty Cash', 'Long Term Assets (Reserves)', 'Long Term Assets (Financial Property)', 'Long Term Receivables: External (Partners)', 'Short Term Receivables: External (Members)', 'Short Term Receivables: External (Youth)', 'Short Term Receivables: External (Partners)', 'Long Term Assets (Property)', 'Long Term Assets (Other)', 'Long Term Liabilities: External (Partners)', 'Long Term Liabilities: External (Other Externals)', 'Short Term Liabilities: External (Members)', 'Short Term Liabilities: External (Other Externals)', 'Prepaid Expenses', 'Allowance for Uncollectible Accounts (contra account)', 'Prepaid Incomes', 'Miscellaneous Revenue', 'Uncollectible Accounts Costs', 'Miscellaneous Costs', 'Overhead Costs: (HR)']

# Initialize an empty DataFrame to store the results
output = pd.DataFrame()

def selected_entity_data(df, entity_id, currency_code, start_date, end_date):
    # filter the DataFrame by entity_id and currency_code
    entity_df = df[(df['entity_id'] == entity_id) & (df['currency_code'] == currency_code)]

    # filter the DataFrame by a date range
    selected_entity_df = entity_df[entity_df['submitted_date'].between(start_date, end_date)]

    # print the filtered DataFrame
    return selected_entity_df

def detect_outliers_iqr_positions(data, first_percentile, third_percentile, threshold=1.5):
    q1 = np.percentile(data, first_percentile)   # Calculate the first quartile of the data
    q3 = np.percentile(data, third_percentile)   # Calculate the third quartile of the data
    iqr = q3 - q1                  # Calculate the interquartile range (IQR) of the data
    lower_bound = q1 - (iqr * threshold)   # Calculate the lower bound for outlier detection using the threshold
    upper_bound = q3 + (iqr * threshold)   # Calculate the upper bound for outlier detection using the threshold
    data_copy = data.copy()  # Make a copy of the input data
    outliers = np.where((data_copy < lower_bound) | (data_copy > upper_bound))[0]   # Find the indices of the outliers in the data array
    return outliers


from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
import io
import os

# Set up the credentials
creds = None
current_dir = os.path.dirname(os.path.abspath(__file__))
creds_path = os.path.join(current_dir, "credentials.json")
if os.path.exists(creds_path):
    creds = Credentials.from_authorized_user_file(creds_path, ['https://www.googleapis.com/auth/drive'])

# Set up the Drive API service
drive_service = build('drive', 'v3', credentials=creds)

# Open the files by name
file_names = ['Cluster.csv', "Email Address.csv"]
folder_id = '1B87PmWwBo8ktHj47KX6OY53kCfpuzoba'  # folder id instead of folder name

result_dfs = []
for file_name in file_names:
    # Get the file id by name
    file_id = None
    query = f"name='{file_name}' and trashed = false and parents in '{folder_id}'"
    response = drive_service.files().list(q=query).execute()
    for file in response.get('files', []):
        if file['name'] == file_name:
            file_id = file['id']
            break

    # Raise an exception if the file is not found
    if not file_id:
        raise ValueError(f"File '{file_name}' not found in folder '{folder_id}'")

    # Download the file content
    file_content = drive_service.files().get_media(fileId=file_id).execute()
    csv_data = file_content
    result_df = pd.read_csv(io.BytesIO(csv_data), encoding='latin1')
    result_dfs.append(result_df)

# Join the data frames based on the entity_id column
information_df = pd.merge(result_dfs[0], result_dfs[1], on=['entity_id', 'entity_name', 'Region'])

# Print the result data frame
print(information_df)

# Set up the credentials
creds = None
creds_path = os.path.join(current_dir, "credentials.json")
if os.path.exists(creds_path):
    creds = Credentials.from_authorized_user_file(creds_path, ['https://www.googleapis.com/auth/gmail.send'])

# Set up the Gmail API service
service = build('gmail', 'v1', credentials=creds)

iqr_dictionary = [{"cluster":"Cluster 1","first_quantile":25, "third_quantile":75, "threshold":1.5},
                  {"cluster":"Cluster 2","first_quantile":20, "third_quantile":80, "threshold":1.6},
                  {"cluster":"Cluster 3","first_quantile":40, "third_quantile":60, "threshold":1.2}]
final_df = pd.DataFrame()

# Loop through each unique entity ID
for entity_id in information_df['entity_id'].unique():
    # Filter the DataFrame by the current entity ID
    entity_df = df[df['entity_id'] == entity_id]

    # Get the corresponding email address and entity name

    email_address = information_df.loc[information_df['entity_id'] == entity_id, 'email_address'].iloc[0]
    entity_name = information_df.loc[information_df['entity_id'] == entity_id, 'entity_name'].iloc[0]
    cluster = information_df.loc[information_df['entity_id'] == entity_id, 'Cluster'].iloc[0]
    # Filter the DataFrame by currency code and date range
    selected_entity_df = selected_entity_data(entity_df, entity_id, currency_code, start_date, end_date)

    if cluster == "Cluster 1":
            iqr_values = iqr_dictionary[0]
    elif cluster == "Cluster 2":
            iqr_values = iqr_dictionary[1]
    elif cluster == "Cluster 3":
            iqr_values = iqr_dictionary[2]

    first_quantile = iqr_values["first_quantile"]
    third_quantile = iqr_values["third_quantile"]
    thres_hold = iqr_values["threshold"]

    # Check that selected_entity_df is not empty
    if len(selected_entity_df) > 0:
        # Loop through each check account
        for check_account in check_accounts:
            # Perform outlier detection on the 'Miscellaneous Costs' column of the selected_entity_df DataFrame
            result_positions = detect_outliers_iqr_positions(selected_entity_df[check_account], first_quantile, third_quantile, threshold=thres_hold)
            result_positions = pd.Series(result_positions)
            result_positions = result_positions.dropna()
            outlier_row = selected_entity_df.iloc[result_positions].reset_index()
            outlier_date = outlier_row['submitted_date']

            # Add the entity ID and outlier positions to the output DataFrame
            entity_output = pd.DataFrame({'entity_id': [entity_id] * len(result_positions),
                                          'account': check_account,
                                          'submitted_date': outlier_date})
            output = pd.concat([output, entity_output])

        # Sort the output DataFrame by entity ID and outlier position
    output = output.sort_values(['account','submitted_date'])
    final_df = pd.concat([final_df, output])
    output = pd.DataFrame()

filename = 'outliers_detection_result.xlsx'
path = os.path.join(current_dir, filename)
final_df = final_df.to_excel(path)

# emails = ["huynhminhnam1999@gmail.com", "minhnam.huynh@aiesec.net"]
emails = ["gfbchair@ai.aiesec.org", "maryn@ai.aiesec.org", "shahin.saatov@aiesec.net", "minhnam.huynh@aiesec.net"]

for email_address in emails:
    # Compose the message
    msg = MIMEMultipart()
    msg['to'] = email_address
    msg['subject'] = "Test send CSV attachment of Outlier Detection Result"

    # Add body text
    body = MIMEText('''Hello,

I would like to share with you the results of the outlier analysis.
Please find the attached CSV file containing the outlier accounts for the specified 12-month range. These accounts have been identified as unusual or abnormal based on the data within that time period. In simpler terms, they are accounts that show values that are significantly different from what is typically observed.

For this month, we are sending the output for you to check and understand the situation. For the next months, it will be required to provide detailed information and your interpretations regarding why these outliers emerged in the accounts.

Best regards,
''', 'plain')
    msg.attach(body)

    # Add attachment
    # filename = 'outliers_detection_result.xlsx'
    # path = os.path.join(current_dir, filename)
    attachment = MIMEBase('application', 'octet-stream')
    with open(path, 'rb') as file:
        attachment.set_payload(file.read())
    encoders.encode_base64(attachment)
    attachment.add_header('Content-Disposition', 'attachment', filename=filename)
    msg.attach(attachment)

    # Convert message to raw format
    raw_msg = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    # Send the message
    try:
        message = service.users().messages().send(userId='me', body={'raw': raw_msg}).execute()
        print('Message Id: %s' % message['id'])

        # Delete the file
        # os.remove(path)

    except HttpError as error:
        print('An error occurred: %s' % error)