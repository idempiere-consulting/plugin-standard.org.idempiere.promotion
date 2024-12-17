/******************************************************************************
 * Copyright (C) 2009 Low Heng Sin                                            *
 * Copyright (C) 2009 Idalica Corporation                                     *
 * This program is free software; you can redistribute it and/or modify it    *
 * under the terms version 2 of the GNU General Public License as published   *
 * by the Free Software Foundation. This program is distributed in the hope   *
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the implied *
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.           *
 * See the GNU General Public License for more details.                       *
 * You should have received a copy of the GNU General Public License along    *
 * with this program; if not, write to the Free Software Foundation, Inc.,    *
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.                     *
 *****************************************************************************/
package org.idempiere.model;

import org.adempiere.base.event.AbstractEventHandler;
import org.adempiere.base.event.IEventTopics;
import org.adempiere.exceptions.AdempiereException;
import org.compiere.model.MDocType;
import org.compiere.model.MInvoice;
import org.compiere.model.MInvoiceLine;
import org.compiere.model.MOrder;
import org.compiere.model.MOrderLine;
import org.compiere.model.MSysConfig;
import org.compiere.model.PO;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.osgi.service.event.Event;

/**
 * Model validator for {@link MOrder} for application of promotion rules.
 * @author hengsin
 * @contributor: <a href="mailto:victor.suarez.is@gmail.com">Ing. Victor Suarez</a>
 * 
 */
public class PromotionValidator extends AbstractEventHandler {
	
	@Override
	protected void initialize() {
		registerTableEvent(IEventTopics.PO_AFTER_DELETE, MOrderLine.Table_Name);
		// iDempiereConsulting __29/10/2018 -- Promotion/CONAI calcolate prima, per includerle nel calcolo delle scadenze
		//registerTableEvent(IEventTopics.DOC_AFTER_PREPARE, MOrder.Table_Name);
		registerTableEvent(IEventTopics.DOC_BEFORE_PREPARE, MOrder.Table_Name);
		// iDempiereConsulting __29/10/2018 ----END
		registerTableEvent(IEventTopics.DOC_AFTER_VOID, MOrder.Table_Name);
		// iDempiereConsulting __ 14/09/2018 -- Promotion a livello di invoiceLine o livello di Invoice
		registerTableEvent(IEventTopics.DOC_BEFORE_PREPARE, MInvoice.Table_Name);
		registerTableEvent(IEventTopics.PO_AFTER_DELETE, MInvoiceLine.Table_Name);
		
	}
	
	@Override
	protected void doHandleEvent(Event event) {
		PO po = getPO(event);
		String type = event.getTopic();
		
		if (po instanceof MOrder ) {
			MOrder order = (MOrder) po;
			if (IEventTopics.DOC_BEFORE_PREPARE.equals(type)) {// iDempiereConsulting __29/10/2018 --
				try {
					PromotionRule.applyPromotions(order);
					order.getLines(true, null);
					order.calculateTaxTotal();
					order.saveEx();
					increasePromotionCounter(order);
				} catch (Exception e) {
					if (e instanceof RuntimeException)
						throw (RuntimeException)e;
					else
						throw new AdempiereException(e.getLocalizedMessage(), e);
				}
			} else if (IEventTopics.DOC_AFTER_VOID.equals(type)) {
				decreasePromotionCounter(order);
			}
		} else if(po instanceof MOrderLine) {
			MOrderLine orderLine = (MOrderLine) po;
			if(IEventTopics.PO_AFTER_DELETE.equals(type)) {
				MOrder order = orderLine.getParent();
				String promotionCode = order.getPromotionCode();
				//if (orderLine.getC_Charge_ID() > 0) { // iDempiereConsulting __ 14/09/2018 -- 
					int promotionID = orderLine.get_ValueAsInt("M_Promotion_ID");
					if (promotionID > 0) {
						int M_PromotionPreCondition_ID = findPromotionPreConditionId(
								order, promotionCode, promotionID);
						if (M_PromotionPreCondition_ID > 0) {
							String update = "UPDATE M_PromotionPreCondition SET PromotionCounter = PromotionCounter - 1 WHERE M_PromotionPreCondition_ID = ?";
							DB.executeUpdate(update, M_PromotionPreCondition_ID, order.get_TrxName());
						}
					}
				//}
			}
		}
		// iDempiereConsulting __ 14/09/2018 -------
		else if(po instanceof MInvoice) {
			if (IEventTopics.DOC_BEFORE_PREPARE.equals(type)) {
				MInvoice invoice = (MInvoice) po;
				try {
					//iDempiereConsulting __28/01/2021 --- Aggiungere un prodotto di tipo BOLLO alla fattura di vendita (NO nota di accredito), in base alla variabile di sistema "LIT_ProductStampDuty" (Imposta di bollo)
					String codValue = null;
					String[] arrayCodValue = null;
					int productID = -1;
					boolean isAddProdStampDuty = MSysConfig.getBooleanValue("LIT_ProductStampDuty", false, invoice.getAD_Client_ID());
					MDocType docType = new MDocType(Env.getCtx(), invoice.getDocTypeID(), null);
					if(isAddProdStampDuty && invoice.isSOTrx() && (MDocType.DOCBASETYPE_ARInvoice.equals(docType.getDocBaseType()))) {
						codValue = DB.getSQLValueString(null, "SELECT Description FROM AD_SysConfig WHERE AD_Client_ID=? AND Name=?", invoice.getAD_Client_ID(), "LIT_ProductStampDuty");
						if(codValue!=null && !codValue.trim().isEmpty()) {
							arrayCodValue = codValue.split(";");
							if(arrayCodValue.length==2) {
								productID = Integer.parseInt(arrayCodValue[0]);
								//Elimino l'eventuale invoiceLine di Bollo
								DB.executeUpdate("DELETE FROM C_InvoiceLine WHERE AD_Client_ID=? AND C_Invoice_ID=? AND M_Product_ID=?", 
										new Object[] {invoice.getAD_Client_ID(), invoice.getC_Invoice_ID(), productID},true, null);
								
							}
						}
					}
					//iDempiereConsulting __28/01/2021 --------- END
					PromotionRuleInvoice.applyPromotions(invoice);
					invoice.getLines(true);
					invoice.calculateTaxTotal();
					invoice.saveEx();
					increasePromotionCounter(invoice);
				} catch (Exception e) {
					if (e instanceof RuntimeException)
						throw (RuntimeException)e;
					else
						throw new AdempiereException(e.getLocalizedMessage(), e);
				}
			} else if (IEventTopics.DOC_AFTER_VOID.equals(type)) {
				MInvoice invoice = (MInvoice) po;
				decreasePromotionCounter(invoice);
			}
			
		}
		else if(po instanceof MInvoiceLine) {
			if(IEventTopics.PO_AFTER_DELETE.equals(type)) {
				MInvoiceLine il = (MInvoiceLine) po;
				MInvoice invoice = il.getParent();
				String promotionCode = (String)invoice.get_Value("PromotionCode");
				//if (ol.getC_Charge_ID() > 0) { // iDempiereConsulting __ 14/09/2018 -- 
					Integer promotionID = (Integer) il.get_Value("M_Promotion_ID");
					if (promotionID != null && promotionID.intValue() > 0) {
						int M_PromotionPreCondition_ID = findPromotionPreConditionId(invoice, promotionCode, promotionID);
						if (M_PromotionPreCondition_ID > 0) {
							String update = "UPDATE M_PromotionPreCondition SET PromotionCounter = PromotionCounter - 1 WHERE M_PromotionPreCondition_ID = ?";
							DB.executeUpdate(update, M_PromotionPreCondition_ID, invoice.get_TrxName());
						}
					}
				//}
			}
		}
		// iDempiereConsulting __ 14/09/2018 -------END
	}

	/**
	 * Increase M_PromotionPreCondition.PromotionCounter value
	 * @param order
	 */
	private void increasePromotionCounter(MOrder order) {
		MOrderLine[] lines = order.getLines(false, null);
		String promotionCode = order.getPromotionCode();
		for (MOrderLine ol : lines) {
			//if (ol.getC_Charge_ID() > 0) {     // iDempiereConsulting __ 14/09/2018 -- 
				int promotionID = ol.get_ValueAsInt("M_Promotion_ID");
				if (promotionID > 0) {

					int M_PromotionPreCondition_ID = findPromotionPreConditionId(
							order, promotionCode, promotionID);
					if (M_PromotionPreCondition_ID > 0) {
						String update = "UPDATE M_PromotionPreCondition SET PromotionCounter = PromotionCounter + 1 WHERE M_PromotionPreCondition_ID = ?";
						DB.executeUpdate(update, M_PromotionPreCondition_ID, order.get_TrxName());
					}
				}
//			}
		}
	}

	/**
	 * Decrease M_PromotionPreCondition.PromotionCounter value
	 * @param order
	 */
	private void decreasePromotionCounter(MOrder order) {
		MOrderLine[] lines = order.getLines(false, null);
		String promotionCode = order.getPromotionCode();
		for (MOrderLine ol : lines) {
			//if (ol.getC_Charge_ID() > 0) {    // iDempiereConsulting __ 14/09/2018 -- 
				int promotionID = ol.get_ValueAsInt("M_Promotion_ID");
				if (promotionID > 0) {
					int M_PromotionPreCondition_ID = findPromotionPreConditionId(
							order, promotionCode, promotionID);
					if (M_PromotionPreCondition_ID > 0) {
						String update = "UPDATE M_PromotionPreCondition SET PromotionCounter = PromotionCounter - 1 WHERE M_PromotionPreCondition_ID = ?";
						DB.executeUpdate(update, M_PromotionPreCondition_ID, order.get_TrxName());
					}
				}
			//}
		}
	}

	// iDempiereConsulting __ 14/09/2018 ---------- 
	private void increasePromotionCounter(MInvoice invoice) {
		MInvoiceLine[] lines = invoice.getLines(false);
		String promotionCode = (String)invoice.get_Value("PromotionCode");
		for (MInvoiceLine il : lines) {
			//if (il.getC_Charge_ID() > 0) {    // iDempiereConsulting __ 14/09/2018 -- 
				Integer promotionID = (Integer) il.get_Value("M_Promotion_ID");
				if (promotionID != null && promotionID.intValue() > 0) {
					int M_PromotionPreCondition_ID = findPromotionPreConditionId(invoice, promotionCode, promotionID);
					if (M_PromotionPreCondition_ID > 0) {
						String update = "UPDATE M_PromotionPreCondition SET PromotionCounter = PromotionCounter + 1 WHERE M_PromotionPreCondition_ID = ?";
						DB.executeUpdate(update, M_PromotionPreCondition_ID, invoice.get_TrxName());
					}
				}
			//}
		}
	}

	private void decreasePromotionCounter(MInvoice invoice) {
		MInvoiceLine[] lines = invoice.getLines(false);
		String promotionCode = (String)invoice.get_Value("PromotionCode");
		for (MInvoiceLine il : lines) {
			//if (il.getC_Charge_ID() > 0) {    // iDempiereConsulting __ 14/09/2018 -- 
				Integer promotionID = (Integer) il.get_Value("M_Promotion_ID");
				if (promotionID != null && promotionID.intValue() > 0) {
					int M_PromotionPreCondition_ID = findPromotionPreConditionId(invoice, promotionCode, promotionID);
					if (M_PromotionPreCondition_ID > 0) {
						String update = "UPDATE M_PromotionPreCondition SET PromotionCounter = PromotionCounter - 1 WHERE M_PromotionPreCondition_ID = ?";
						DB.executeUpdate(update, M_PromotionPreCondition_ID, invoice.get_TrxName());
					}
				}
			//}
		}
	}
	// iDempiereConsulting __ 14/09/2018 ----------END

	/**
	 * @param order
	 * @param promotionCode
	 * @param promotionID
	 * @return M_PromotionPreCondition_ID
	 */
	private int findPromotionPreConditionId(MOrder order, String promotionCode,
			int promotionID) {
		String bpFilter = "M_PromotionPreCondition.C_BPartner_ID = ? OR M_PromotionPreCondition.C_BP_Group_ID = ? OR (M_PromotionPreCondition.C_BPartner_ID IS NULL AND M_PromotionPreCondition.C_BP_Group_ID IS NULL)";
		String priceListFilter = "M_PromotionPreCondition.M_PriceList_ID IS NULL OR M_PromotionPreCondition.M_PriceList_ID = ?";
		String warehouseFilter = "M_PromotionPreCondition.M_Warehouse_ID IS NULL OR M_PromotionPreCondition.M_Warehouse_ID = ?";
		String dateFilter = "M_PromotionPreCondition.StartDate <= ? AND (M_PromotionPreCondition.EndDate >= ? OR M_PromotionPreCondition.EndDate IS NULL)";

		StringBuilder select = new StringBuilder();
		select.append(" SELECT M_PromotionPreCondition.M_PromotionPreCondition_ID FROM M_PromotionPreCondition ")
			.append(" WHERE")
			.append(" (" + bpFilter + ")")
			.append(" AND (").append(priceListFilter).append(")")
			.append(" AND (").append(warehouseFilter).append(")")
			.append(" AND (").append(dateFilter).append(")")
			.append(" AND (M_PromotionPreCondition.M_Promotion_ID = ?)")
			.append(" AND (M_PromotionPreCondition.LIT_isPromForOrder = 'Y')")// iDempiereConsulting __ 14/09/2018 --
			.append(" AND (M_PromotionPreCondition.IsActive = 'Y')");
		if (promotionCode != null && promotionCode.trim().length() > 0) {
			select.append(" AND (M_PromotionPreCondition.PromotionCode = ?)");
		} else {
			select.append(" AND (M_PromotionPreCondition.PromotionCode IS NULL)");
		}
		select.append(" ORDER BY M_PromotionPreCondition.C_BPartner_ID Desc, M_PromotionPreCondition.C_BP_Group_ID Desc, M_PromotionPreCondition.M_PriceList_ID Desc, M_PromotionPreCondition.M_Warehouse_ID Desc, M_PromotionPreCondition.StartDate Desc");
		int M_PromotionPreCondition_ID = 0;
		int C_BP_Group_ID = 0;
		try {
			C_BP_Group_ID = order.getC_BPartner().getC_BP_Group_ID();
		} catch (Exception e) {
		}
		if (promotionCode != null && promotionCode.trim().length() > 0) {
			M_PromotionPreCondition_ID = DB.getSQLValue(order.get_TrxName(), select.toString(), order.getC_BPartner_ID(),
					C_BP_Group_ID, order.getM_PriceList_ID(), order.getM_Warehouse_ID(), order.getDateOrdered(),
					order.getDateOrdered(), promotionID, promotionCode);
		} else {
			M_PromotionPreCondition_ID = DB.getSQLValue(order.get_TrxName(), select.toString(), order.getC_BPartner_ID(),
					C_BP_Group_ID, order.getM_PriceList_ID(), order.getM_Warehouse_ID(), order.getDateOrdered(),
					order.getDateOrdered(), promotionID);
		}
		return M_PromotionPreCondition_ID;
	}

	// iDempiereConsulting __ 14/09/2018 ---------- 
	private int findPromotionPreConditionId(MInvoice invoice, String promotionCode,
			Integer promotionID) {
		String bpFilter = "M_PromotionPreCondition.C_BPartner_ID = ? OR M_PromotionPreCondition.C_BP_Group_ID = ? OR (M_PromotionPreCondition.C_BPartner_ID IS NULL AND M_PromotionPreCondition.C_BP_Group_ID IS NULL)";
		String priceListFilter = "M_PromotionPreCondition.M_PriceList_ID IS NULL OR M_PromotionPreCondition.M_PriceList_ID = ?";
		//String warehouseFilter = "M_PromotionPreCondition.M_Warehouse_ID IS NULL OR M_PromotionPreCondition.M_Warehouse_ID = ?";
		String dateFilter = "M_PromotionPreCondition.StartDate <= ? AND (M_PromotionPreCondition.EndDate >= ? OR M_PromotionPreCondition.EndDate IS NULL)";

		StringBuilder select = new StringBuilder();
		select.append(" SELECT M_PromotionPreCondition.M_PromotionPreCondition_ID FROM M_PromotionPreCondition ")
			.append(" WHERE")
			.append(" (" + bpFilter + ")")
			.append(" AND (").append(priceListFilter).append(")")
			//.append(" AND (").append(warehouseFilter).append(")")
			.append(" AND (").append(dateFilter).append(")")
			.append(" AND (M_PromotionPreCondition.M_Promotion_ID = ?)")
			.append(" AND (M_PromotionPreCondition.LIT_isPromForInvoice = 'Y')")
			.append(" AND (M_PromotionPreCondition.IsActive = 'Y')");
		if (promotionCode != null && promotionCode.trim().length() > 0) {
			select.append(" AND (M_PromotionPreCondition.PromotionCode = ?)");
		} else {
			select.append(" AND (M_PromotionPreCondition.PromotionCode IS NULL)");
		}
		select.append(" ORDER BY M_PromotionPreCondition.C_BPartner_ID Desc, M_PromotionPreCondition.C_BP_Group_ID Desc, M_PromotionPreCondition.M_PriceList_ID Desc, M_PromotionPreCondition.M_Warehouse_ID Desc, M_PromotionPreCondition.StartDate Desc");
		int M_PromotionPreCondition_ID = 0;
		int C_BP_Group_ID = 0;
		try {
			C_BP_Group_ID = invoice.getC_BPartner().getC_BP_Group_ID();
		} catch (Exception e) {
		}
		if (promotionCode != null && promotionCode.trim().length() > 0) {
			M_PromotionPreCondition_ID = DB.getSQLValue(invoice.get_TrxName(), select.toString(), invoice.getC_BPartner_ID(),
					C_BP_Group_ID, invoice.getM_PriceList_ID(),  invoice.getDateInvoiced(), invoice.getDateInvoiced(), promotionID, promotionCode);
		} else {
			M_PromotionPreCondition_ID = DB.getSQLValue(invoice.get_TrxName(), select.toString(), invoice.getC_BPartner_ID(),
					C_BP_Group_ID, invoice.getM_PriceList_ID(), invoice.getDateInvoiced(), invoice.getDateInvoiced(), promotionID);
		}
		return M_PromotionPreCondition_ID;
	}
	// iDempiereConsulting __ 14/09/2018 ----------END

}
