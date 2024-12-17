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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import org.adempiere.exceptions.AdempiereException;
import org.compiere.model.MInvoice;
import org.compiere.model.MInvoiceLine;
import org.compiere.model.MPriceList;
import org.compiere.model.MPriceListVersion;
import org.compiere.model.MProduct;
import org.compiere.model.MProductPrice;
import org.compiere.model.MTable;
import org.compiere.model.MUOM;
import org.compiere.model.MUOMConversion;
import org.compiere.model.Query;
import org.compiere.model.X_M_ProductPriceVendorBreak;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.Msg;

/**
 *
 * @author Andrea Checchia
 *
 */
public class PromotionRuleInvoice {
	
	private static TreeMap<String, List<MInvoiceLine>> listConsolid = new TreeMap<String, List<MInvoiceLine>>();
	private static List<MInvoiceLine> listCount_il = null;
	private static ArrayList<Integer> listTaxID = new ArrayList<Integer>(); // per recupero promotionID+listTAXID

	public static void applyPromotions(MInvoice invoice) throws Exception {
		// iDempiereConsulting __ 23/05/2018 -- Promotion a livello di invoiceLine o livello di Invoice
		boolean isConsolid;
		boolean isVATCalcLine = false;
		boolean isSupplPromotion = false;
		MInvoiceLine[] lines_1 = invoice.getLines();
		
		//cacheReset
		if(listConsolid!=null)
			listConsolid.clear();
		if(listCount_il!=null)
			listCount_il.clear();
		if(listTaxID!=null)
			listTaxID.clear();
		//

		Map<Integer, List<Integer>> promotions = null;
		Map<Integer, BigDecimal> invoiceLineQty = null;

		boolean hasDeleteLine = false;
		for (MInvoiceLine il : lines_1) {
			Number id = (Number) il.get_Value("M_Promotion_ID");
			if (id != null && id.intValue() > 0) {
				il.delete(false);
				hasDeleteLine = true;
			}
		}

		//refresh order
		if (hasDeleteLine) {
			lines_1 =  invoice.getLines(true);
			invoice.getTaxes(true);
			invoice.setGrandTotal(DB.getSQLValueBD(invoice.get_TrxName(), "SELECT GrandTotal From C_Invoice WHERE C_Invoice_ID = ?", invoice.getC_Invoice_ID()));

		}


		for (MInvoiceLine mInvoiceLine : lines_1) {

			invoiceLineQty = new LinkedHashMap<Integer, BigDecimal>();
			promotions = PromotionRuleInvoice.findM_Promotion_IDByOrderLine(mInvoiceLine);
			if (promotions == null || promotions.isEmpty()) continue;

			BigDecimal invoiceAmount = mInvoiceLine.getLineNetAmt();
			invoiceLineQty.put(mInvoiceLine.getC_InvoiceLine_ID(), mInvoiceLine.getQtyInvoiced());

			//key = M_PromotionDistribution_ID, value = C_OrderLine_ID and Qty
			Map<Integer, DistributionSet> distributions = new LinkedHashMap<Integer, DistributionSet>();

			//distribute order lines
			for (Map.Entry<Integer, List<Integer>> entry : promotions.entrySet()) {
				Query query = new Query(Env.getCtx(), MTable.get(invoice.getCtx(), I_M_PromotionDistribution.Table_ID),
						"M_PromotionDistribution.M_Promotion_ID = ? AND M_PromotionDistribution.IsActive = 'Y'", invoice.get_TrxName());
				query.setParameters(new Object[]{entry.getKey()});
				query.setOrderBy("SeqNo");
				List<MPromotionDistribution> list = query.<MPromotionDistribution>list();

				Query rewardQuery = new Query(Env.getCtx(), MTable.get(invoice.getCtx(), I_M_PromotionReward.Table_ID),
						"M_PromotionReward.M_Promotion_ID = ? AND M_PromotionReward.IsActive = 'Y'", invoice.get_TrxName());
				rewardQuery.setParameters(new Object[]{entry.getKey()});
				rewardQuery.setOrderBy("SeqNo");
				List<MPromotionReward> rewardList = rewardQuery.<MPromotionReward>list();

				List<MPromotionLine> promotionLines = new ArrayList<MPromotionLine>();
				for (Integer M_PromotionLine_ID : entry.getValue()) {
					MPromotionLine promotionLine = new MPromotionLine(invoice.getCtx(), M_PromotionLine_ID, invoice.get_TrxName());
					promotionLines.add(promotionLine);
				}
				while (true) {
					boolean hasDistributionSet = false;
					Set<Integer>promotionLineSet = new HashSet<Integer>();
					Set<Integer>mandatoryLineSet = new HashSet<Integer>();
					boolean mandatoryLineNotFound = false;
					List<Integer> validPromotionLineIDs = new ArrayList<Integer>();
					for (MPromotionLine promotionLine : promotionLines) {
						if (promotionLine.getM_PromotionGroup_ID() == 0 && promotionLine.getMinimumAmt() != null && promotionLine.getMinimumAmt().signum() >= 0) {
							if (invoiceAmount.compareTo(promotionLine.getMinimumAmt()) >= 0) {
								invoiceAmount = invoiceAmount.subtract(promotionLine.getMinimumAmt());
								validPromotionLineIDs.add(promotionLine.getM_PromotionLine_ID());
							} else if (promotionLine.isMandatoryPL()) {
								mandatoryLineNotFound = true;
								break;
							}
						}
					}
					if (mandatoryLineNotFound) {
						break;
					}
					for (MPromotionDistribution pd : list) {
						if (entry.getValue().contains(pd.getM_PromotionLine_ID())) {
							//sort available invoiceline base on distribution sorting type
							List<Integer> invoiceLineIdList = new ArrayList<Integer>();
							invoiceLineIdList.add(mInvoiceLine.getC_InvoiceLine_ID());

							DistributionSet prevSet = distributions.get(pd.getM_PromotionDistribution_ID());
							DistributionSet distributionSet = PromotionRuleInvoice.calculateDistributionQty(pd, prevSet, validPromotionLineIDs, invoiceLineQty, invoiceLineIdList, invoice.get_TrxName());
							if (distributionSet != null && distributionSet.setQty.signum() > 0) {
								hasDistributionSet = true;
								promotionLineSet.add(pd.getM_PromotionLine_ID());
							} else {
								if (pd.getM_PromotionLine().isMandatoryPL()) {
									mandatoryLineSet.add(pd.getM_PromotionLine_ID());
								}
							}
							distributions.put(pd.getM_PromotionDistribution_ID(), distributionSet);
						}
					}
					if (!hasDistributionSet)
						break;

					if (mandatoryLineSet != null) {
						mandatoryLineNotFound = false;
						for(Integer id : mandatoryLineSet) {
							if (!promotionLineSet.contains(id)) {
								mandatoryLineNotFound = true;
								break;
							}
						}
						if (mandatoryLineNotFound) {
							break;
						}
					}

					for (MPromotionReward pr : rewardList) {
						if (pr.isForAllDistribution()) {
							Collection<DistributionSet> all = distributions.values();
							BigDecimal totalPrice = BigDecimal.ZERO;
							for(DistributionSet distributionSet : all) {
								for(Map.Entry<Integer, BigDecimal> ilMap : distributionSet.invoiceLines.entrySet()) {
									BigDecimal qty = (BigDecimal) ilMap.getValue();
									int C_InvoiceLine_ID = (Integer) ilMap.getKey();
									if (mInvoiceLine.getC_InvoiceLine_ID() == C_InvoiceLine_ID) {
										totalPrice = totalPrice.add(mInvoiceLine.getPriceActual().multiply(qty));
										break;
									}
									distributionSet.invoiceLines.put(ilMap.getKey(), BigDecimal.ZERO);
								}
							}
							MProduct product_CONAI = null;
							MProductPrice prodPrice = null;
							X_M_ProductPriceVendorBreak ppvb = null;
							BigDecimal discount = BigDecimal.ZERO;
							BigDecimal qtyCONAI = BigDecimal.ZERO;
							if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_AbsoluteAmount)) {
								if (pr.getAmount().compareTo(totalPrice) < 0) {
									discount = totalPrice.subtract(pr.getAmount());
								}
							} else if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_FlatDiscount)) {
								discount = pr.getAmount();
							} else if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_Percentage)) {
								//iDempiereConsulting __ 09/10/2018 -- Calcolo o no dell'IVA a livello riga promozione/CONAI
								//is VAT Calculated Line --- IVA calcolata nella linea
								isVATCalcLine = pr.get_ValueAsBoolean("LIT_isVATCalcLine");
								//
								
								// iDempiereConsulting __ 11/09/2018 -- Promotion CONAI
								if(pr.get_ValueAsBoolean("LIT_isCalcWeight") && pr.get_ValueAsInt("M_Product_ID")>0) {
									isConsolid = pr.get_ValueAsBoolean("LIT_isConsolidateLine");
									int M_Product_ID = pr.get_ValueAsInt("M_Product_ID");
									product_CONAI = new MProduct(Env.getCtx(), M_Product_ID, null); 


									BigDecimal one = mInvoiceLine.getQtyEntered().multiply(mInvoiceLine.getProduct().getWeight());
									int cUOM_From = DB.getSQLValue(null, "SELECT C_UOM_ID FROM C_UOM WHERE UOMSymbol='Kg' AND UOMType='"+MUOM.UOMTYPE_Weight+"' AND AD_CLient_ID=?", Env.getAD_Client_ID(Env.getCtx()));
									int cUOM_To = DB.getSQLValue(null, "SELECT C_UOM_ID FROM C_UOM WHERE UOMSymbol='t' AND UOMType='"+MUOM.UOMTYPE_Weight+"' AND AD_CLient_ID=?", Env.getAD_Client_ID(Env.getCtx()));
									qtyCONAI = MUOMConversion.convert(cUOM_From, cUOM_To, one, false);

									MPriceList priceList = new MPriceList(Env.getCtx(), mInvoiceLine.getC_Invoice().getM_PriceList_ID(), null);
									MPriceListVersion priceListVersion = priceList.getPriceListVersion(new Timestamp(System.currentTimeMillis()));												
									prodPrice = MProductPrice.get(Env.getCtx(), priceListVersion.getM_PriceList_Version_ID(), M_Product_ID, null);
									int M_ProductPriceVendorBreak_ID = DB.getSQLValue(null, 
											"SELECT M_ProductPriceVendorBreak_ID " +
													"FROM M_ProductPriceVendorBreak " +
													"WHERE M_PriceList_Version_ID=? AND " +
													"IsActive='Y' AND " +
													"C_BPartner_ID=? AND " +
													"M_Product_ID=?", 
													new Object[]{priceListVersion.getM_PriceList_Version_ID(), mInvoiceLine.getC_Invoice().getC_BPartner_ID(), M_Product_ID});
									if (M_ProductPriceVendorBreak_ID > 0)
										ppvb = new X_M_ProductPriceVendorBreak(Env.getCtx(), M_ProductPriceVendorBreak_ID, null);

									addCONAILine(invoice, mInvoiceLine, ppvb, prodPrice, qtyCONAI, product_CONAI, pr.getM_Promotion(), isVATCalcLine, isConsolid);
								}
								else
									discount = pr.getAmount().divide(Env.ONEHUNDRED).multiply(totalPrice);

								// iDempiereConsulting __ 11/09/2018 -- END

							}
							if (discount.signum() > 0) {
								addDiscountLine(invoice, mInvoiceLine, discount, Env.ONE, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
							}
						} else {
							int M_PromotionDistribution_ID = pr.getM_PromotionDistribution_ID();
							if (!distributions.containsKey(M_PromotionDistribution_ID))
								continue;
							int targetDistributionID = M_PromotionDistribution_ID;
							if (!pr.isSameDistribution()) {
								targetDistributionID = pr.getM_TargetDistribution_ID();
								if (!distributions.containsKey(targetDistributionID))
									continue;
							}
							DistributionSet distributionSet = distributions.get(targetDistributionID);

							//sort by reward distribution sorting
							// 									if (pr.getDistributionSorting() != null ) {
							//										Comparator<Integer> cmp = new OrderLineComparator(orderLineIndex);
							//										if (pr.getDistributionSorting().equals(MPromotionReward.DISTRIBUTIONSORTING_Descending))
							//											cmp = Collections.reverseOrder(cmp);
							//										Set<Integer> keySet = distributionSet.orderLines.keySet();
							//										List<Integer> keyList = new ArrayList<Integer>();
							//										keyList.addAll(keySet);
							//										Collections.sort(keyList, cmp);
							//										Map<Integer, BigDecimal>sortedMap = new LinkedHashMap<Integer, BigDecimal>();
							//										for(Integer id : keyList) {
							//											sortedMap.put(id, distributionSet.orderLines.get(id));
							//										}
							//										distributionSet.orderLines = sortedMap;
							//									}


							BigDecimal setBalance = distributionSet.setQty;
							BigDecimal toApply = pr.getQty();
							if (toApply == null || toApply.signum() == 0)
								toApply = BigDecimal.valueOf(-1.0);

							BigDecimal totalPrice  = BigDecimal.ZERO;

							for(Map.Entry<Integer, BigDecimal> ilMap : distributionSet.invoiceLines.entrySet()) {
								BigDecimal qty = ilMap.getValue();
								int C_InvoiceLine_ID = ilMap.getKey();
								if (qty == null || qty.signum() <= 0)
									continue;
								if (qty.compareTo(setBalance) >= 0) {
									qty = setBalance;
									setBalance = BigDecimal.ZERO;
								} else {
									setBalance = setBalance.subtract(qty);
								}
								if (toApply.signum() > 0) {
									if (toApply.compareTo(qty) <= 0) {
										qty = toApply;
										toApply = BigDecimal.ZERO;
									} else {
										toApply = toApply.subtract(qty);
									}
									BigDecimal newQty = ilMap.getValue();
									newQty = newQty.subtract(qty);
									distributionSet.invoiceLines.put(ilMap.getKey(), newQty);
								}
								if (mInvoiceLine.getC_OrderLine_ID() == C_InvoiceLine_ID) {
									if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_Percentage)) {
										BigDecimal priceActual = mInvoiceLine.getPriceActual();
										BigDecimal discount = priceActual.multiply(pr.getAmount().divide(Env.ONEHUNDRED));
										addDiscountLine(invoice, mInvoiceLine, discount, qty, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
									} else if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_FlatDiscount)) {
										addDiscountLine(invoice, mInvoiceLine, pr.getAmount(), Env.ONE, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
									} else if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_AbsoluteAmount)) {
										BigDecimal priceActual = mInvoiceLine.getPriceActual();
										totalPrice = totalPrice.add(priceActual.multiply(qty));
									}
								}

								if (toApply.signum() == 0)
									break;
								if (setBalance.signum() == 0)
									break;
							}
							if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_AbsoluteAmount))  {
								if (pr.getAmount().compareTo(totalPrice) < 0) {
									addDiscountLine(invoice, null, totalPrice.subtract(pr.getAmount()), Env.ONE, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
								}
							}
						}
					}
				}
			}
		}

		isSupplPromotion = true;

		//refresh
		invoice.getLines(true);
		invoice.getTaxes(true);
		invoice.setGrandTotal(DB.getSQLValueBD(invoice.get_TrxName(), "SELECT GrandTotal From C_Invoice WHERE C_Invoice_ID = ?", invoice.getC_Invoice_ID()));
		//
		//key = C_InvoiceLine, value = Qty to distribution
		MInvoiceLine[] lines = invoice.getLines();
		// ------- 11/09/2018
		/*
			Map<Integer, BigDecimal> invoiceLineQty = new LinkedHashMap<Integer, BigDecimal>();
			Map<Integer, MInvoiceLine> invoiceLineIndex = new HashMap<Integer, MInvoiceLine>();

			boolean hasDeleteLine = false;
		 */
		invoiceLineQty = new LinkedHashMap<Integer, BigDecimal>();
		Map<Integer, MInvoiceLine> invoiceLineIndex = new HashMap<Integer, MInvoiceLine>();

		hasDeleteLine = false;
		// ------- 11/09/2018  END.....
		for (MInvoiceLine il : lines) {
			if (il.getM_Product_ID() > 0) {
				if (il.getQtyInvoiced().signum() > 0) {
					invoiceLineQty.put(il.getC_InvoiceLine_ID(), il.getQtyInvoiced());
					invoiceLineIndex.put(il.getC_InvoiceLine_ID(), il);
				}

			} else if ((il.getC_Charge_ID() > 0) && (il.getQtyEntered().compareTo(BigDecimal.ZERO))!=0) {
				Number id = (Number) il.get_Value("M_Promotion_ID");
				if (id != null && id.intValue() > 0 && !isSupplPromotion) {
					il.delete(false);
					hasDeleteLine = true;
				}
				else {
					invoiceLineQty.put(il.getC_InvoiceLine_ID(), il.getQtyInvoiced());
					invoiceLineIndex.put(il.getC_InvoiceLine_ID(), il);
				}

			}
		}
		if (invoiceLineQty.isEmpty()) return;

		//refresh order
		if (hasDeleteLine) {
			invoice.getLines(true);
			invoice.getTaxes(true);
			invoice.setGrandTotal(DB.getSQLValueBD(invoice.get_TrxName(), "SELECT GrandTotal From C_Invoice WHERE C_Order_ID = ?", invoice.getC_Invoice_ID()));
		}

		//			Map<Integer, List<Integer>> promotions = PromotionRule.findM_Promotion_ID(order);  ------- 11/09/2018
		promotions = PromotionRuleInvoice.findM_Promotion_ID(invoice);
		// ------- 11/09/2018  END.....

		if (promotions == null || promotions.isEmpty()) return;

		BigDecimal invoiceAmount = invoice.getGrandTotal();

		//key = M_PromotionDistribution_ID, value = C_OrderLine_ID and Qty
		Map<Integer, DistributionSet> distributions = new LinkedHashMap<Integer, DistributionSet>();

		//<M_PromotionDistribution_ID, DistributionSorting>
		Map<Integer, String> sortingType = new HashMap<Integer, String>();
		InvoiceLineComparator olComparator = new InvoiceLineComparator(invoiceLineIndex);
		//distribute order lines
		for (Map.Entry<Integer, List<Integer>> entry : promotions.entrySet()) {
			Query query = new Query(Env.getCtx(), MTable.get(invoice.getCtx(), I_M_PromotionDistribution.Table_ID),
					"M_PromotionDistribution.M_Promotion_ID = ? AND M_PromotionDistribution.IsActive = 'Y'", invoice.get_TrxName());
			query.setParameters(new Object[]{entry.getKey()});
			query.setOrderBy("SeqNo");
			List<MPromotionDistribution> list = query.<MPromotionDistribution>list();

			Query rewardQuery = new Query(Env.getCtx(), MTable.get(invoice.getCtx(), I_M_PromotionReward.Table_ID),
					"M_PromotionReward.M_Promotion_ID = ? AND M_PromotionReward.IsActive = 'Y'", invoice.get_TrxName());
			rewardQuery.setParameters(new Object[]{entry.getKey()});
			rewardQuery.setOrderBy("SeqNo");
			List<MPromotionReward> rewardList = rewardQuery.<MPromotionReward>list();

			List<MPromotionLine> promotionLines = new ArrayList<MPromotionLine>();
			for (Integer M_PromotionLine_ID : entry.getValue()) {
				MPromotionLine promotionLine = new MPromotionLine(invoice.getCtx(), M_PromotionLine_ID, invoice.get_TrxName());
				promotionLines.add(promotionLine);
			}
			while (true) {
				boolean hasDistributionSet = false;
				Set<Integer>promotionLineSet = new HashSet<Integer>();
				Set<Integer>mandatoryLineSet = new HashSet<Integer>();
				boolean mandatoryLineNotFound = false;
				List<Integer> validPromotionLineIDs = new ArrayList<Integer>();
				for (MPromotionLine promotionLine : promotionLines) {
					if (promotionLine.getM_PromotionGroup_ID() == 0 && promotionLine.getMinimumAmt() != null && promotionLine.getMinimumAmt().signum() >= 0) {
						if (invoiceAmount.compareTo(promotionLine.getMinimumAmt()) >= 0) {
							invoiceAmount = invoiceAmount.subtract(promotionLine.getMinimumAmt());
							validPromotionLineIDs.add(promotionLine.getM_PromotionLine_ID());
						} else if (promotionLine.isMandatoryPL()) {
							mandatoryLineNotFound = true;
							break;
						}
					}
				}
				if (mandatoryLineNotFound) {
					break;
				}
				for (MPromotionDistribution pd : list) {
					if (entry.getValue().contains(pd.getM_PromotionLine_ID())) {
						//sort available orderline base on distribution sorting type
						List<Integer> invoiceLineIdList = new ArrayList<Integer>();
						invoiceLineIdList.addAll(invoiceLineQty.keySet());
						if (pd.getDistributionSorting() != null) {
							Comparator<Integer> cmp = olComparator;
							if (pd.getDistributionSorting().equals(MPromotionDistribution.DISTRIBUTIONSORTING_Descending))
								cmp = Collections.reverseOrder(cmp);
							Collections.sort(invoiceLineIdList, cmp);
						}
						DistributionSet prevSet = distributions.get(pd.getM_PromotionDistribution_ID());
						DistributionSet distributionSet = PromotionRuleInvoice.calculateDistributionQty(pd, prevSet, validPromotionLineIDs, invoiceLineQty, invoiceLineIdList, invoice.get_TrxName());
						if (distributionSet != null && distributionSet.setQty.signum() > 0) {
							hasDistributionSet = true;
							promotionLineSet.add(pd.getM_PromotionLine_ID());
						} else {
							if (pd.getM_PromotionLine().isMandatoryPL()) {
								mandatoryLineSet.add(pd.getM_PromotionLine_ID());
							}
						}
						distributions.put(pd.getM_PromotionDistribution_ID(), distributionSet);
						sortingType.put(pd.getM_PromotionDistribution_ID(), pd.getDistributionSorting());
					}
				}
				if (!hasDistributionSet)
					break;

				if (mandatoryLineSet != null) {
					mandatoryLineNotFound = false;
					for(Integer id : mandatoryLineSet) {
						if (!promotionLineSet.contains(id)) {
							mandatoryLineNotFound = true;
							break;
						}
					}
					if (mandatoryLineNotFound) {
						break;
					}
				}

				for (MPromotionReward pr : rewardList) {
					if (pr.isForAllDistribution()) {
						Collection<DistributionSet> all = distributions.values();
						BigDecimal totalPrice = BigDecimal.ZERO;
						for(DistributionSet distributionSet : all) {
							for(Map.Entry<Integer, BigDecimal> ilMap : distributionSet.invoiceLines.entrySet()) {
								BigDecimal qty = (BigDecimal) ilMap.getValue();
								int C_InvoiceLine_ID = (Integer) ilMap.getKey();
								for (MInvoiceLine il : lines) {
									if (il.getC_InvoiceLine_ID() == C_InvoiceLine_ID) {
										totalPrice = totalPrice.add(il.getPriceActual().multiply(qty));
										break;
									}
								}
								distributionSet.invoiceLines.put(ilMap.getKey(), BigDecimal.ZERO);
							}
						}
						BigDecimal discount = BigDecimal.ZERO;
						if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_AbsoluteAmount)) {
							if (pr.getAmount().compareTo(totalPrice) < 0) {
								discount = totalPrice.subtract(pr.getAmount());
							}
						} else if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_FlatDiscount)) {
							discount = pr.getAmount();
						} else if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_Percentage)) {
							discount = pr.getAmount().divide(Env.ONEHUNDRED).multiply(totalPrice);
						}
						if (discount.signum() > 0) {
							// iDempiereConsulting __ 05/09/2018 -- Promotion a livello di Invoice con o senza l'imponibile
							if(pr.get_ValueAsBoolean("LIT_isCalcWithVAT")) 
								discount = pr.getAmount().divide(Env.ONEHUNDRED).multiply(invoice.getGrandTotal());
							// iDempiereConsulting __ 05/09/2018 -- END
							addDiscountLine(invoice, null, discount, Env.ONE, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
						}
					} else {
						int M_PromotionDistribution_ID = pr.getM_PromotionDistribution_ID();
						if (!distributions.containsKey(M_PromotionDistribution_ID))
							continue;
						int targetDistributionID = M_PromotionDistribution_ID;
						if (!pr.isSameDistribution()) {
							targetDistributionID = pr.getM_TargetDistribution_ID();
							if (!distributions.containsKey(targetDistributionID))
								continue;
						}
						DistributionSet distributionSet = distributions.get(targetDistributionID);

						//sort by reward distribution sorting
						if (pr.getDistributionSorting() != null ) {
							Comparator<Integer> cmp = new InvoiceLineComparator(invoiceLineIndex);
							if (pr.getDistributionSorting().equals(MPromotionReward.DISTRIBUTIONSORTING_Descending))
								cmp = Collections.reverseOrder(cmp);
							Set<Integer> keySet = distributionSet.invoiceLines.keySet();
							List<Integer> keyList = new ArrayList<Integer>();
							keyList.addAll(keySet);
							Collections.sort(keyList, cmp);
							Map<Integer, BigDecimal>sortedMap = new LinkedHashMap<Integer, BigDecimal>();
							for(Integer id : keyList) {
								sortedMap.put(id, distributionSet.invoiceLines.get(id));
							}
							distributionSet.invoiceLines = sortedMap;
						}


						BigDecimal setBalance = distributionSet.setQty;
						BigDecimal toApply = pr.getQty();
						if (toApply == null || toApply.signum() == 0)
							toApply = BigDecimal.valueOf(-1.0);

						BigDecimal totalPrice  = BigDecimal.ZERO;

						for(Map.Entry<Integer, BigDecimal> ilMap : distributionSet.invoiceLines.entrySet()) {
							BigDecimal qty = ilMap.getValue();
							int C_InvoiceLine_ID = ilMap.getKey();
							if (qty == null || qty.signum() <= 0)
								continue;
							if (qty.compareTo(setBalance) >= 0) {
								qty = setBalance;
								setBalance = BigDecimal.ZERO;
							} else {
								setBalance = setBalance.subtract(qty);
							}
							if (toApply.signum() > 0) {
								if (toApply.compareTo(qty) <= 0) {
									qty = toApply;
									toApply = BigDecimal.ZERO;
								} else {
									toApply = toApply.subtract(qty);
								}
								BigDecimal newQty = ilMap.getValue();
								newQty = newQty.subtract(qty);
								distributionSet.invoiceLines.put(ilMap.getKey(), newQty);
							}
							for (MInvoiceLine il : lines) {
								if (il.getC_InvoiceLine_ID() == C_InvoiceLine_ID) {
									if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_Percentage)) {
										BigDecimal priceActual = il.getPriceActual();
										BigDecimal discount = priceActual.multiply(pr.getAmount().divide(Env.ONEHUNDRED));
										addDiscountLine(invoice, il, discount, qty, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
									} else if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_FlatDiscount)) {
										addDiscountLine(invoice, il, pr.getAmount(), Env.ONE, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
									} else if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_AbsoluteAmount)) {
										BigDecimal priceActual = il.getPriceActual();
										totalPrice = totalPrice.add(priceActual.multiply(qty));
									}
								}
							}

							if (toApply.signum() == 0)
								break;
							if (setBalance.signum() == 0)
								break;
						}
						if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_AbsoluteAmount))  {
							if (pr.getAmount().compareTo(totalPrice) < 0) {
								addDiscountLine(invoice, null, totalPrice.subtract(pr.getAmount()), Env.ONE, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
							}
						}
					}
				}
			}
			// iDempiereConsulting __ 20/09/2018 -- Promotion CONAI ....consolid
			if(/*isConsolid &&*/ listConsolid!=null && !listConsolid.isEmpty()) {

				for (Integer t_ID : listTaxID) {
					String sum = entry.getKey().intValue()+"_"+t_ID.intValue();
					List<MInvoiceLine> listInvoices = (listConsolid.get(sum));
					if(listInvoices != null) {
						MInvoiceLine tmp_line = listInvoices.get(0);
						BigDecimal priceStd = tmp_line.getPriceEntered();
						BigDecimal priceList = tmp_line.getPriceList();
						Integer proID_CONAI = tmp_line.getM_Product_ID();
						Integer uomID_CONAI = tmp_line.getC_UOM_ID();
						Function<MInvoiceLine, BigDecimal> totalMapper = invoiceLine -> invoiceLine.getQtyEntered();
						BigDecimal tot_qty = listInvoices.stream().map(totalMapper).reduce(BigDecimal.ZERO, BigDecimal::add);

						MInvoiceLine nil = new MInvoiceLine(invoice.getCtx(), 0, invoice.get_TrxName());
						nil.setC_Invoice_ID(invoice.getC_Invoice_ID());
						nil.setAD_Org_ID(invoice.getAD_Org_ID());
						nil.setInvoice(invoice);
						nil.setM_Product_ID(proID_CONAI);
						nil.setC_UOM_ID(uomID_CONAI);
						nil.setQty(tot_qty);
						nil.setC_Tax_ID(tmp_line.getC_Tax_ID());
						nil.setPriceEntered(priceStd);
						nil.setPriceActual(priceStd);
						nil.setPriceList(priceList);
						nil.setLineNetAmt();

						nil.setDescription(tmp_line.getProduct().getDescription());
						nil.set_ValueOfColumn("M_Promotion_ID", tmp_line.get_ValueAsInt("M_Promotion_ID"));
						if (tmp_line.getC_Campaign_ID() > 0) {
							nil.setC_Campaign_ID(tmp_line.getC_Campaign_ID());
						}
						if (!nil.save())
							throw new AdempiereException(Msg.getMsg(Env.getCtx(), "LIT_CONAI_invoice"));
					}
				}
			}
			//
		}
		if(listConsolid!=null)
			listConsolid.clear();
		if(listCount_il!=null)
			listCount_il.clear();
		if(listTaxID!=null)
			listTaxID.clear();
		// iDempiereConsulting __ 23/05/2018 -- END
	}

	private static void addCONAILine(MInvoice invoice, MInvoiceLine iLine, X_M_ProductPriceVendorBreak prodPriceBreak, MProductPrice prodPrice2, BigDecimal qty,
			MProduct product_CONAI, I_M_Promotion promotion, boolean isVATCalcLine, boolean isConsolid) throws Exception{
		MInvoiceLine nil = new MInvoiceLine(invoice.getCtx(), 0, invoice.get_TrxName());
		nil.setC_Invoice_ID(invoice.getC_Invoice_ID());
		nil.setAD_Org_ID(invoice.getAD_Org_ID());
		nil.setInvoice(invoice);
		nil.setM_Product_ID(product_CONAI.getM_Product_ID());
		nil.setC_UOM_ID(product_CONAI.getC_UOM_ID());
		nil.setQty(qty);

		BigDecimal priceStd = BigDecimal.ZERO;
		BigDecimal priceList = BigDecimal.ZERO;

		if(prodPriceBreak!=null) {
			priceStd = prodPriceBreak.getPriceStd();
			priceList = prodPriceBreak.getPriceList();
		}
		else if(prodPrice2!=null) {
			priceStd = prodPrice2.getPriceStd();
			priceList = prodPrice2.getPriceList();
		}
		else
			throw new AdempiereException(Msg.getMsg(Env.getCtx(), "LIT_CONAI_OrderProduct")+" "+product_CONAI.getName());

		//iDempiereConsulting __ 09/10/2018 -- Calcolo o no dell'IVA a livello riga promozione/CONAI
		if(isVATCalcLine && iLine != null)
			nil.setC_Tax_ID(iLine.getC_Tax_ID());
		else
			nil.setTax();
		//iDempiereConsulting __ 09/10/2018 -- END
		
		nil.setPriceEntered(priceStd);
		nil.setPriceActual(priceStd);
		nil.setPriceList(priceList);

		nil.setLineNetAmt();

		String description = product_CONAI.getName();

		nil.setDescription(description + " --- " + iLine.getProduct().getValue()+" ___ "+iLine.getProduct().getName());
		nil.set_ValueOfColumn("M_Promotion_ID", promotion.getM_Promotion_ID());
		if (promotion.getC_Campaign_ID() > 0) {
			nil.setC_Campaign_ID(promotion.getC_Campaign_ID());
		}


		if(isConsolid) {
			String sum = promotion.getM_Promotion_ID()+"_"+nil.getC_Tax_ID(); //key per recupero dati di consolida in unica riga per tasse diverse
			if(listConsolid.containsKey(sum)) {
				(listConsolid.get(sum)).add(nil);
			}
			else {
				listCount_il = new ArrayList<MInvoiceLine>();
				listCount_il.add(nil);
				listConsolid.put(sum, listCount_il);
			}
			
			if(!listTaxID.contains(nil.getC_Tax_ID()))
				listTaxID.add(nil.getC_Tax_ID());
				
		}
		else {
			if (!nil.save())
				throw new AdempiereException(Msg.getMsg(Env.getCtx(), "LIT_CONAI_invoice"));
		}
	}

	private static void addDiscountLine(MInvoice invoice, MInvoiceLine il, BigDecimal discount,
			BigDecimal qty, int C_Charge_ID, I_M_Promotion promotion, boolean isVATCalcLine) throws Exception {
		MInvoiceLine nil = new MInvoiceLine(invoice.getCtx(), 0, invoice.get_TrxName());
		nil.setC_Invoice_ID(invoice.getC_Invoice_ID());
		nil.setAD_Org_ID(invoice.getAD_Org_ID());
		nil.setInvoice(invoice);
		nil.setC_Charge_ID(C_Charge_ID);
		nil.setQty(qty);
		if (discount.scale() > 2)
			discount = discount.setScale(2, RoundingMode.HALF_UP);
		
		//iDempiereConsulting __ 09/10/2018 -- Calcolo o no dell'IVA a livello riga promozione/CONAI
		if(isVATCalcLine && il != null) 
			nil.setC_Tax_ID(il.getC_Tax_ID());
		//iDempiereConsulting __ 09/10/2018 -- END
		
		nil.setPriceEntered(discount.negate());
		nil.setPriceActual(discount.negate());
		if (il != null && Integer.toString(il.getLine()).endsWith("0")) {
			for(int i = 0; i < 9; i++) {
				int line = il.getLine() + i + 1;
				int r = DB.getSQLValue(invoice.get_TrxName(), "SELECT C_InvoiceLine_ID FROM C_InvoiceLine WHERE C_Invoice_ID = ? AND Line = ?", invoice.getC_Invoice_ID(), line);
				if (r <= 0) {
					nil.setLine(line);
					break;
				}
			}
		}
		String description = promotion.getName();
		if (il != null)
			description += (", " + il.getName());
		nil.setDescription(description);
		nil.set_ValueOfColumn("M_Promotion_ID", promotion.getM_Promotion_ID());
		if (promotion.getC_Campaign_ID() > 0) {
			nil.setC_Campaign_ID(promotion.getC_Campaign_ID());
		}
		if (!nil.save())
			throw new AdempiereException(Msg.getMsg(Env.getCtx(), "LIT_CONAI_discount"));
	}

	/**
	 *
	 * @param order
	 * @return Map<M_Promotion_ID, List<M_PromotionLine_ID>>
	 * @throws Exception
	 */
	private static Map<Integer, List<Integer>> findM_Promotion_ID(MInvoice invoice) throws Exception {
		String select = "SELECT M_Promotion.M_Promotion_ID From M_Promotion Inner Join M_PromotionPreCondition "
				+ " ON (M_Promotion.M_Promotion_ID = M_PromotionPreCondition.M_Promotion_ID)";

		String bpFilter = "M_PromotionPreCondition.C_BPartner_ID = ? OR M_PromotionPreCondition.C_BP_Group_ID = ? OR (M_PromotionPreCondition.C_BPartner_ID IS NULL AND M_PromotionPreCondition.C_BP_Group_ID IS NULL)";
		String priceListFilter = "M_PromotionPreCondition.M_PriceList_ID IS NULL OR M_PromotionPreCondition.M_PriceList_ID = ?";
		//		String warehouseFilter = "M_PromotionPreCondition.M_Warehouse_ID IS NULL OR M_PromotionPreCondition.M_Warehouse_ID = ?";
		String dateFilter = "M_PromotionPreCondition.StartDate <= ? AND (M_PromotionPreCondition.EndDate >= ? OR M_PromotionPreCondition.EndDate IS NULL)";

		//optional promotion code filter
		String promotionCode = (String)invoice.get_Value("PromotionCode");

		StringBuilder sql = new StringBuilder();
		sql.append(select)
		.append(" WHERE")
		.append(" (" + bpFilter + ")")
		.append(" AND (").append(priceListFilter).append(")")
		//			.append(" AND (").append(warehouseFilter).append(")")
		.append(" AND (").append(dateFilter).append(")");
		if (promotionCode != null && promotionCode.trim().length() > 0) {
			sql.append(" AND (M_PromotionPreCondition.PromotionCode = ?)");
		} else {
			sql.append(" AND (M_PromotionPreCondition.PromotionCode IS NULL)");
		}

		//optional activity filter
		int C_Activity_ID = invoice.getC_Activity_ID();
		if (C_Activity_ID > 0) {
			sql.append(" AND (M_PromotionPreCondition.C_Activity_ID = ? OR M_PromotionPreCondition.C_Activity_ID IS NULL)");
		} else {
			sql.append(" AND (M_PromotionPreCondition.C_Activity_ID IS NULL)");
		}

		sql.append(" AND (M_Promotion.AD_Client_ID in (0, ?))")
		.append(" AND (M_Promotion.AD_Org_ID in (0, ?))")
		.append(" AND (M_Promotion.IsActive = 'Y')")
		.append(" AND (M_PromotionPreCondition.IsActive = 'Y')")
		.append(" AND (M_PromotionPreCondition.LIT_isPromForInvoice = 'Y')")
		.append(" AND (M_PromotionPreCondition.isSOTrx = '"+((invoice.isSOTrx())?"Y":"N")+"')")
		.append(" ORDER BY M_Promotion.PromotionPriority Desc ");

		PreparedStatement stmt = null;
		ResultSet rs = null;
		//Key = M_Promotion_ID, value = List<M_PromotionLine_ID>
		Map<Integer, List<Integer>> promotions = new LinkedHashMap<Integer, List<Integer>>();
		try {
			int pindex = 1;
			stmt = DB.prepareStatement(sql.toString(), invoice.get_TrxName());
			stmt.setInt(pindex++, invoice.getC_BPartner_ID());
			stmt.setInt(pindex++, invoice.getC_BPartner().getC_BP_Group_ID());
			stmt.setInt(pindex++, invoice.getM_PriceList_ID());
			//			stmt.setInt(pindex++, order.getM_Warehouse_ID());
			stmt.setTimestamp(pindex++, invoice.getDateInvoiced());
			stmt.setTimestamp(pindex++, invoice.getDateInvoiced());
			if (promotionCode != null && promotionCode.trim().length() > 0) {
				stmt.setString(pindex++, promotionCode);
			}
			if (C_Activity_ID > 0) {
				stmt.setInt(pindex++, C_Activity_ID);
			}
			stmt.setInt(pindex++, invoice.getAD_Client_ID());
			stmt.setInt(pindex++, invoice.getAD_Org_ID());
			rs = stmt.executeQuery();
			while(rs.next()) {
				int M_Promotion_ID = rs.getInt(1);
				List<Integer> promotionLineIDs = findPromotionLine(M_Promotion_ID, invoice);
				if (!promotionLineIDs.isEmpty()) {
					promotions.put(M_Promotion_ID, promotionLineIDs);
				}
			}
		} finally {
			DB.close(rs, stmt);
		}

		return promotions;
	}

	/**
	 *
	 * @param order
	 * @return Map<M_Promotion_ID, List<M_PromotionLine_ID>>
	 * @throws Exception
	 */
	private static Map<Integer, List<Integer>> findM_Promotion_IDByOrderLine(MInvoiceLine iLine) throws Exception {
		MInvoice invoice = new MInvoice(Env.getCtx(), iLine.getC_Invoice_ID(), null);

		String select = "SELECT M_Promotion.M_Promotion_ID From M_Promotion Inner Join M_PromotionPreCondition "
				+ " ON (M_Promotion.M_Promotion_ID = M_PromotionPreCondition.M_Promotion_ID)";

		String bpFilter = "M_PromotionPreCondition.C_BPartner_ID = ? OR M_PromotionPreCondition.C_BP_Group_ID = ? OR (M_PromotionPreCondition.C_BPartner_ID IS NULL AND M_PromotionPreCondition.C_BP_Group_ID IS NULL)";
		String priceListFilter = "M_PromotionPreCondition.M_PriceList_ID IS NULL OR M_PromotionPreCondition.M_PriceList_ID = ?";
		//		String warehouseFilter = "M_PromotionPreCondition.M_Warehouse_ID IS NULL OR M_PromotionPreCondition.M_Warehouse_ID = ?";
		String dateFilter = "M_PromotionPreCondition.StartDate <= ? AND (M_PromotionPreCondition.EndDate >= ? OR M_PromotionPreCondition.EndDate IS NULL)";

		//optional promotion code filter
		String promotionCode = (String)iLine.get_Value("PromotionCode");

		StringBuilder sql = new StringBuilder();
		sql.append(select)
		.append(" WHERE")
		.append(" (" + bpFilter + ")")
		.append(" AND (").append(priceListFilter).append(")")
		//			.append(" AND (").append(warehouseFilter).append(")")
		.append(" AND (").append(dateFilter).append(")");
		if (promotionCode != null && promotionCode.trim().length() > 0) {
			sql.append(" AND (M_PromotionPreCondition.PromotionCode = ?)");
		} else {
			sql.append(" AND (M_PromotionPreCondition.PromotionCode IS NULL)");
		}


		//optional activity filter
		int C_Activity_ID = iLine.getC_Activity_ID();
		if (C_Activity_ID > 0) {
			sql.append(" AND (M_PromotionPreCondition.C_Activity_ID = ? OR M_PromotionPreCondition.C_Activity_ID IS NULL)");
		} else {
			sql.append(" AND (M_PromotionPreCondition.C_Activity_ID IS NULL)");
		}

		sql.append(" AND (M_Promotion.AD_Client_ID in (0, ?))")
		.append(" AND (M_Promotion.AD_Org_ID in (0, ?))")
		.append(" AND (M_Promotion.IsActive = 'Y')")
		.append(" AND (M_PromotionPreCondition.IsActive = 'Y')")
		.append(" AND (M_PromotionPreCondition.LIT_isPromForInvoice = 'Y')")
		.append(" AND (M_PromotionPreCondition.isSOTrx = '"+((invoice.isSOTrx())?"Y":"N")+"')")
		.append(" ORDER BY M_Promotion.PromotionPriority Desc ");

		PreparedStatement stmt = null;
		ResultSet rs = null;
		//Key = M_Promotion_ID, value = List<M_PromotionLine_ID>
		Map<Integer, List<Integer>> promotions = new LinkedHashMap<Integer, List<Integer>>();
		try {
			int pindex = 1;
			stmt = DB.prepareStatement(sql.toString(), iLine.get_TrxName());
			stmt.setInt(pindex++, iLine.getC_Invoice().getC_BPartner_ID());
			stmt.setInt(pindex++, iLine.getC_Invoice().getC_BPartner().getC_BP_Group_ID());
			stmt.setInt(pindex++, iLine.getC_Invoice().getM_PriceList_ID());
			//			stmt.setInt(pindex++, iLine.getM_Warehouse_ID());
			stmt.setTimestamp(pindex++, iLine.getC_Invoice().getDateInvoiced());
			stmt.setTimestamp(pindex++, iLine.getC_Invoice().getDateInvoiced());
			if (promotionCode != null && promotionCode.trim().length() > 0) {
				stmt.setString(pindex++, promotionCode);
			}
			if (C_Activity_ID > 0) {
				stmt.setInt(pindex++, C_Activity_ID);
			}
			stmt.setInt(pindex++, iLine.getC_Invoice().getAD_Client_ID());
			stmt.setInt(pindex++, iLine.getC_Invoice().getAD_Org_ID());
			rs = stmt.executeQuery();
			while(rs.next()) {
				int M_Promotion_ID = rs.getInt(1);
				List<Integer> promotionLineIDs = findPromotionLine(M_Promotion_ID, invoice);
				if (!promotionLineIDs.isEmpty()) {
					promotions.put(M_Promotion_ID, promotionLineIDs);
				}
			}
		} finally {
			DB.close(rs, stmt);
		}

		return promotions;
	}


	/**
	 *
	 * @param distribution
	 * @param prevSet
	 * @param orderLineQty
	 * @param orderLineQty2
	 * @param orderLineIdList
	 * @param qtyAvailable
	 * @return Distribution Qty
	 * @throws Exception
	 */
	private static DistributionSet calculateDistributionQty(MPromotionDistribution distribution,
			DistributionSet prevSet, List<Integer> validPromotionLineIDs, Map<Integer, BigDecimal> invoiceLineQty, List<Integer> invoiceLineIdList, String trxName) throws Exception {

		String sql = "SELECT C_InvoiceLine.C_InvoiceLine_ID FROM M_PromotionLine"
				+ " INNER JOIN M_PromotionGroup ON (M_PromotionLine.M_PromotionGroup_ID = M_PromotionGroup.M_PromotionGroup_ID AND M_PromotionGroup.IsActive = 'Y')"
				+ " INNER JOIN M_PromotionGroupLine ON (M_PromotionGroup.M_PromotionGroup_ID = M_PromotionGroupLine.M_PromotionGroup_ID AND M_PromotionGroupLine.IsActive = 'Y')"
				+ " INNER JOIN C_InvoiceLine ON (M_PromotionGroupLine.M_Product_ID = C_InvoiceLine.M_Product_ID)"
				+ " WHERE M_PromotionLine.M_PromotionLine_ID = ? AND C_InvoiceLine.C_InvoiceLine_ID = ?"
				+ " AND M_PromotionLine.IsActive = 'Y'";


		DistributionSet distributionSet = new DistributionSet();
		List<Integer>eligibleInvoiceLineIDs = new ArrayList<Integer>();
		if (distribution.getM_PromotionLine().getM_PromotionGroup_ID() == 0) {
			if (validPromotionLineIDs.contains(distribution.getM_PromotionLine_ID())) {
				eligibleInvoiceLineIDs.addAll(invoiceLineIdList);
			}
		} else {
			for(int C_InvoiceLine_ID : invoiceLineIdList) {
				BigDecimal availableQty = invoiceLineQty.get(C_InvoiceLine_ID);
				if (availableQty.signum() <= 0) continue;
				PreparedStatement stmt = null;
				ResultSet rs = null;
				try {
					stmt = DB.prepareStatement(sql, trxName);
					stmt.setInt(1, distribution.getM_PromotionLine_ID());
					stmt.setInt(2, C_InvoiceLine_ID);
					rs = stmt.executeQuery();
					if (rs.next()) {
						eligibleInvoiceLineIDs.add(C_InvoiceLine_ID);
					}
				} catch (Exception e) {
					throw new AdempiereException(e.getLocalizedMessage(), e);
				} finally {
					DB.close(rs, stmt);
				}
			}
		}

		if (eligibleInvoiceLineIDs.isEmpty()) {
			distributionSet.setQty = BigDecimal.ZERO;
			return distributionSet;
		}

		BigDecimal compareQty = distribution.getQty();

		BigDecimal setQty = BigDecimal.ZERO;
		BigDecimal totalInvoiceLineQty = BigDecimal.ZERO;
		for (int C_InvoiceLine_ID : eligibleInvoiceLineIDs) {
			BigDecimal availableQty = invoiceLineQty.get(C_InvoiceLine_ID);
			if (availableQty.signum() <= 0) continue;
			totalInvoiceLineQty = totalInvoiceLineQty.add(availableQty);
		}
		int compare = totalInvoiceLineQty.compareTo(compareQty);
		boolean match = false;
		if (compare <= 0 && "<=".equals(distribution.getOperation())) {
			match = true;
		} else if (compare >= 0 && ">=".equals(distribution.getOperation())) {
			match = true;
		}
		if (match) {
			if (MPromotionDistribution.DISTRIBUTIONTYPE_Max.equals(distribution.getDistributionType())) {
				setQty = compare > 0 ? totalInvoiceLineQty : distribution.getQty();
			} else if (MPromotionDistribution.DISTRIBUTIONTYPE_Min.equals(distribution.getDistributionType())) {
				setQty = compare < 0 ? totalInvoiceLineQty : distribution.getQty();
			} else {
				setQty = compare > 0 ? totalInvoiceLineQty.subtract(distribution.getQty())
						: distribution.getQty().subtract(totalInvoiceLineQty);
			}
			distributionSet.setQty = setQty;
			while (setQty.signum() > 0) {
				if (prevSet != null) {
					BigDecimal recycleQty = BigDecimal.ZERO;
					for(Map.Entry<Integer, BigDecimal> entry : prevSet.invoiceLines.entrySet()) {
						if (entry.getValue().signum() > 0) {
							setQty = setQty.subtract(entry.getValue());
							distributionSet.invoiceLines.put(entry.getKey(), entry.getValue());
							recycleQty = recycleQty.add(entry.getValue());
						}
					}
					if (recycleQty.signum() > 0) {
						for (int C_InvoiceLine_ID : eligibleInvoiceLineIDs) {
							BigDecimal availableQty = invoiceLineQty.get(C_InvoiceLine_ID);
							if (availableQty.signum() <= 0) continue;
							if (availableQty.compareTo(recycleQty) < 0) {
								recycleQty = recycleQty.subtract(availableQty);
								invoiceLineQty.put(C_InvoiceLine_ID, BigDecimal.ZERO);
							} else {
								availableQty = availableQty.subtract(recycleQty);
								invoiceLineQty.put(C_InvoiceLine_ID, availableQty);
								recycleQty = BigDecimal.ZERO;
							}
							if (recycleQty.signum() <= 0)
								break;
						}
					}
					if (setQty.signum() == 0) break;
				}
				for (int C_InvoiceLine_ID : eligibleInvoiceLineIDs) {
					BigDecimal availableQty = invoiceLineQty.get(C_InvoiceLine_ID);
					if (availableQty.signum() <= 0) continue;
					if (availableQty.compareTo(setQty) < 0) {
						setQty = setQty.subtract(availableQty);
						distributionSet.invoiceLines.put(C_InvoiceLine_ID, availableQty);
						invoiceLineQty.put(C_InvoiceLine_ID, BigDecimal.ZERO);
					} else {
						availableQty = availableQty.subtract(setQty);
						distributionSet.invoiceLines.put(C_InvoiceLine_ID, setQty);
						invoiceLineQty.put(C_InvoiceLine_ID, availableQty);
						setQty = BigDecimal.ZERO;
					}
					if (setQty.signum() <= 0)
						break;
				}
			}
		}

		return distributionSet ;
	}

	/**
	 *
	 * @param promotion_ID
	 * @param order
	 * @return List<M_PromotionLine_ID>
	 * @throws SQLException
	 */
	private static List<Integer> findPromotionLine(int promotion_ID, MInvoice invoice) throws SQLException {
		Query query = new Query(Env.getCtx(), MTable.get(invoice.getCtx(), I_M_PromotionLine.Table_ID), " M_PromotionLine.M_Promotion_ID = ? AND M_PromotionLine.IsActive = 'Y'", invoice.get_TrxName());
		query.setParameters(new Object[]{promotion_ID});
		List<MPromotionLine>plist = query.<MPromotionLine>list();
		//List<M_PromotionLine_ID>
		List<Integer>applicable = new ArrayList<Integer>();
		MInvoiceLine[] lines = invoice.getLines();
		String sql = "SELECT DISTINCT C_InvoiceLine.C_InvoiceLine_ID FROM M_PromotionGroup INNER JOIN M_PromotionGroupLine"
				+ " ON (M_PromotionGroup.M_PromotionGroup_ID = M_PromotionGroupLine.M_PromotionGroup_ID AND M_PromotionGroupLine.IsActive = 'Y')"
				+ " INNER JOIN C_InvoiceLine ON (M_PromotionGroupLine.M_Product_ID = C_InvoiceLine.M_Product_ID)"
				+ " INNER JOIN M_PromotionLine ON (M_PromotionLine.M_PromotionGroup_ID = M_PromotionGroup.M_PromotionGroup_ID)"
				+ " WHERE M_PromotionLine.M_PromotionLine_ID = ? AND C_InvoiceLine.C_Invoice_ID = ?"
				+ " AND M_PromotionLine.IsActive = 'Y'"
				+ " AND M_PromotionGroup.IsActive = 'Y'";
		for (MPromotionLine pl : plist) {
			boolean match = false;
			if (pl.getM_PromotionGroup_ID() > 0) {

				PreparedStatement stmt = null;
				ResultSet rs = null;
				try {
					stmt = DB.prepareStatement(sql, invoice.get_TrxName());
					stmt.setInt(1, pl.getM_PromotionLine_ID());
					stmt.setInt(2, invoice.getC_Invoice_ID());
					rs = stmt.executeQuery();
					BigDecimal invoiceAmt = BigDecimal.ZERO;
					while(rs.next()) {
						if (pl.getMinimumAmt() != null && pl.getMinimumAmt().signum() > 0) {
							int C_InvoiceLine_ID = rs.getInt(1);
							for (MInvoiceLine il : lines) {
								if (il.getC_InvoiceLine_ID() == C_InvoiceLine_ID) {
									invoiceAmt = invoiceAmt.add(il.getLineNetAmt());
									break;
								}
							}
							if (invoiceAmt.compareTo(pl.getMinimumAmt()) >= 0) {
								match = true;
								break;
							}
						} else {
							match = true;
							break;
						}
					}
				} finally {
					DB.close(rs, stmt);
				}
			} else if (pl.getMinimumAmt() != null && pl.getMinimumAmt().compareTo(invoice.getGrandTotal()) <= 0 ) {
				match = true;
			}
			if (!match && pl.isMandatoryPL()) {
				applicable.clear();
				break;
			}
			if (match)
				applicable.add(pl.getM_PromotionLine_ID());
		}
		return applicable;
	}

	static class DistributionSet {
		//<C_OrderLine_Id, DistributionQty>
		Map<Integer, BigDecimal> invoiceLines = new LinkedHashMap<Integer, BigDecimal>();
		BigDecimal setQty = BigDecimal.ZERO;
	}

	static class InvoiceLineComparator implements Comparator<Integer> {
		Map<Integer, MInvoiceLine> index;
		InvoiceLineComparator(Map<Integer, MInvoiceLine> olIndex) {
			index = olIndex;
		}

		public int compare(Integer ol1, Integer ol2) {
			return index.get(ol1).getPriceActual().compareTo(index.get(ol2).getPriceActual());
		}
	}
}
