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
import org.compiere.model.MOrder;
import org.compiere.model.MOrderLine;
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
 * Static helper methods for promotion rule (M_Promotion)
 * @author hengsin
 * @contributor: <a href="mailto:victor.suarez.is@gmail.com">Ing. Victor Suarez</a>
 * 
 */
public class PromotionRule {
	
	private static TreeMap<String, List<MOrderLine>> listConsolid = new TreeMap<String, List<MOrderLine>>();
	private static List<MOrderLine> listCount_ol = null;
	private static ArrayList<Integer> listTaxID = new ArrayList<Integer>(); // per recupero promotionID+listTAXID

	/**
	 * Apply promotion rules to order
	 * @param order
	 * @throws Exception
	 */
	public static void applyPromotions(MOrder order) throws Exception {
		// iDempiereConsulting __ 23/05/2018 -- Promotion a livello di orderLine o livello di Order
		boolean isConsolid;
		boolean isVATCalcLine = false;
		boolean isSupplPromotion = false;
		MOrderLine[] lines_1 = order.getLines();
		List<Integer> noPromoDuplicate = new ArrayList<Integer>();
		
		//cacheReset
		if(listConsolid!=null)
			listConsolid.clear();
		if(listCount_ol!=null)
			listCount_ol.clear();
		if(listTaxID!=null)
			listTaxID.clear();
		//

		Map<Integer, List<Integer>> promotions = null;
		Map<Integer, BigDecimal> orderLineQty = null;

		boolean hasDeleteLine = false;
		for (MOrderLine ol : lines_1) {
			Number id = (Number) ol.get_Value("M_Promotion_ID");
			if (id != null && id.intValue() > 0) {
				ol.setQtyReserved(BigDecimal.ZERO);
				boolean noDuplicate= ol.delete(false);
				//iDempiereConsulting __02/12/2019 --- Se in caso la promo presente sull'ordine è gia legata ad un DDT, evito la duplicazione in caso di riapertura dell'ordine
				if(!noDuplicate) {
					noPromoDuplicate.add(id.intValue());
				}
				///////
				hasDeleteLine = true;
			}
		}

		//refresh order
		if (hasDeleteLine) {
			lines_1 =  order.getLines(true, null);
			order.getTaxes(true);
			order.setGrandTotal(DB.getSQLValueBD(order.get_TrxName(), "SELECT GrandTotal From C_Order WHERE C_Order_ID = ?", order.getC_Order_ID()));

		}


		for (MOrderLine mOrderLine : lines_1) {

			orderLineQty = new LinkedHashMap<Integer, BigDecimal>();
			promotions = PromotionRule.findM_Promotion_IDByOrderLine(mOrderLine);
			if ((promotions == null || promotions.isEmpty())) continue;

			BigDecimal orderAmount = mOrderLine.getLineNetAmt();
			orderLineQty.put(mOrderLine.getC_OrderLine_ID(), mOrderLine.getQtyOrdered());


			//key = M_PromotionDistribution_ID, value = C_OrderLine_ID and Qty
			Map<Integer, DistributionSet> distributions = new LinkedHashMap<Integer, DistributionSet>();

			//distribute order lines
			for (Map.Entry<Integer, List<Integer>> entry : promotions.entrySet()) {
				//iDempiereConsulting __02/12/2019 --- Se in caso la promo presente sull'ordine è gia legata ad un DDT, evito la duplicazione in caso di riapertura dell'ordine
				if(noPromoDuplicate.contains(entry.getKey()))
					continue;
				/////
				
				Query query = new Query(Env.getCtx(), MTable.get(order.getCtx(), I_M_PromotionDistribution.Table_ID),
						"M_PromotionDistribution.M_Promotion_ID = ? AND M_PromotionDistribution.IsActive = 'Y'", order.get_TrxName());
				query.setParameters(new Object[]{entry.getKey()});
				query.setOrderBy("SeqNo");
				List<MPromotionDistribution> list = query.<MPromotionDistribution>list();

				Query rewardQuery = new Query(Env.getCtx(), MTable.get(order.getCtx(), I_M_PromotionReward.Table_ID),
						"M_PromotionReward.M_Promotion_ID = ? AND M_PromotionReward.IsActive = 'Y'", order.get_TrxName());
				rewardQuery.setParameters(new Object[]{entry.getKey()});
				rewardQuery.setOrderBy("SeqNo");
				List<MPromotionReward> rewardList = rewardQuery.<MPromotionReward>list();

				List<MPromotionLine> promotionLines = new ArrayList<MPromotionLine>();
				for (Integer M_PromotionLine_ID : entry.getValue()) {
					MPromotionLine promotionLine = new MPromotionLine(order.getCtx(), M_PromotionLine_ID, order.get_TrxName());
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
							if (orderAmount.compareTo(promotionLine.getMinimumAmt()) >= 0) {
								orderAmount = orderAmount.subtract(promotionLine.getMinimumAmt());
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
							List<Integer> orderLineIdList = new ArrayList<Integer>();
							orderLineIdList.add(mOrderLine.getC_OrderLine_ID());

							DistributionSet prevSet = distributions.get(pd.getM_PromotionDistribution_ID());
							DistributionSet distributionSet = PromotionRule.calculateDistributionQty(pd, prevSet, validPromotionLineIDs, orderLineQty, orderLineIdList, order.get_TrxName());
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
								for(Map.Entry<Integer, BigDecimal> olMap : distributionSet.orderLines.entrySet()) {
									BigDecimal qty = (BigDecimal) olMap.getValue();
									int C_OrderLine_ID = (Integer) olMap.getKey();
									if (mOrderLine.getC_OrderLine_ID() == C_OrderLine_ID) {
										totalPrice = totalPrice.add(mOrderLine.getPriceActual().multiply(qty));
										break;
									}
									distributionSet.orderLines.put(olMap.getKey(), BigDecimal.ZERO);
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


									BigDecimal one = mOrderLine.getQtyEntered().multiply(mOrderLine.getProduct().getWeight());
									int cUOM_From = DB.getSQLValue(null, "SELECT C_UOM_ID FROM C_UOM WHERE UOMSymbol='Kg' AND UOMType='"+MUOM.UOMTYPE_Weight+"'");
									int cUOM_To = DB.getSQLValue(null, "SELECT C_UOM_ID FROM C_UOM WHERE UOMSymbol='t' AND UOMType='"+MUOM.UOMTYPE_Weight+"'");
									qtyCONAI = MUOMConversion.convert(cUOM_From, cUOM_To, one, false);

									MPriceList priceList = new MPriceList(Env.getCtx(), mOrderLine.getC_Order().getM_PriceList_ID(), null);
									MPriceListVersion priceListVersion = priceList.getPriceListVersion(new Timestamp(System.currentTimeMillis()));												
									prodPrice = MProductPrice.get(Env.getCtx(), priceListVersion.getM_PriceList_Version_ID(), M_Product_ID, null);
									int M_ProductPriceVendorBreak_ID = DB.getSQLValue(null, 
											"SELECT M_ProductPriceVendorBreak_ID " +
													"FROM M_ProductPriceVendorBreak " +
													"WHERE M_PriceList_Version_ID=? AND " +
													"IsActive='Y' AND " +
													"C_BPartner_ID=? AND " +
													"M_Product_ID=?", 
													new Object[]{priceListVersion.getM_PriceList_Version_ID(), mOrderLine.getC_BPartner_ID(), M_Product_ID});
									if (M_ProductPriceVendorBreak_ID > 0)
										ppvb = new X_M_ProductPriceVendorBreak(Env.getCtx(), M_ProductPriceVendorBreak_ID, null);
									
									addCONAILine(order, mOrderLine, ppvb, prodPrice, qtyCONAI, product_CONAI, pr.getM_Promotion(), isVATCalcLine, isConsolid);
								}
								else
									discount = pr.getAmount().divide(Env.ONEHUNDRED).multiply(totalPrice);

								// iDempiereConsulting __ 11/09/2018 -- END

							}
							if (discount.signum() > 0) {
//								addDiscountLine(order, null, discount, Env.ONE, pr.getC_Charge_ID(), pr.getM_Promotion());
								addDiscountLine(order, mOrderLine, discount, Env.ONE, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
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

							for(Map.Entry<Integer, BigDecimal> olMap : distributionSet.orderLines.entrySet()) {
								BigDecimal qty = olMap.getValue();
								int C_OrderLine_ID = olMap.getKey();
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
									BigDecimal newQty = olMap.getValue();
									newQty = newQty.subtract(qty);
									distributionSet.orderLines.put(olMap.getKey(), newQty);
								}
								if (mOrderLine.getC_OrderLine_ID() == C_OrderLine_ID) {
									if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_Percentage)) {
										BigDecimal priceActual = mOrderLine.getPriceActual();
										BigDecimal discount = priceActual.multiply(pr.getAmount().divide(Env.ONEHUNDRED));
										addDiscountLine(order, mOrderLine, discount, qty, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
									} else if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_FlatDiscount)) {
										addDiscountLine(order, mOrderLine, pr.getAmount(), Env.ONE, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
									} else if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_AbsoluteAmount)) {
										BigDecimal priceActual = mOrderLine.getPriceActual();
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
									addDiscountLine(order, null, totalPrice.subtract(pr.getAmount()), Env.ONE, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
								}
							}
						}
					}
				}
			}
		}

		isSupplPromotion = true;

		//refresh
		order.getLines(true, null);
		order.getTaxes(true);
		order.setGrandTotal(DB.getSQLValueBD(order.get_TrxName(), "SELECT GrandTotal From C_Order WHERE C_Order_ID = ?", order.getC_Order_ID()));
		//
		//key = C_OrderLine, value = Qty to distribution
		//Map<Integer, BigDecimal> orderLineQty = new LinkedHashMap<Integer, BigDecimal>();  ------- 11/09/2018
		orderLineQty = new LinkedHashMap<Integer, BigDecimal>();
		Map<Integer, MOrderLine> orderLineIndex = new HashMap<Integer, MOrderLine>();
		MOrderLine[] lines = order.getLines();
		//boolean hasDeleteLine = false;  ------- 11/09/2018
		hasDeleteLine = false;
		for (MOrderLine ol : lines) {
			if (ol.getM_Product_ID() > 0) {
				if (ol.getQtyOrdered().signum() > 0) {
					orderLineQty.put(ol.getC_OrderLine_ID(), ol.getQtyOrdered());
					orderLineIndex.put(ol.getC_OrderLine_ID(), ol);
				}
			} else if (ol.getC_Charge_ID() > 0) {
				int id = ol.get_ValueAsInt("M_Promotion_ID");
				if (id > 0 && !isSupplPromotion) {
					ol.delete(false);
					hasDeleteLine = true;
				}
				// ------- 11/09/2018
				else {
					orderLineQty.put(ol.getC_OrderLine_ID(), ol.getQtyOrdered());
					orderLineIndex.put(ol.getC_OrderLine_ID(), ol);
				}

			}
		}
		if (orderLineQty.isEmpty()) return;

		//refresh order
		if (hasDeleteLine) {
			order.getLines(true, null);
			order.getTaxes(true);
			order.setGrandTotal(DB.getSQLValueBD(order.get_TrxName(), "SELECT GrandTotal From C_Order WHERE C_Order_ID = ?", order.getC_Order_ID()));
		}

		//Map<Integer, List<Integer>> promotions = PromotionRule.findM_Promotion_ID(order);  ------- 11/09/2018
		promotions = PromotionRule.findM_Promotion_ID(order);
		// ------- 11/09/2018  END.....

		if (promotions == null || promotions.isEmpty()) return;

		BigDecimal orderAmount = order.getGrandTotal();

		//key = M_PromotionDistribution_ID, value = C_OrderLine_ID and Qty
		Map<Integer, DistributionSet> distributions = new LinkedHashMap<Integer, DistributionSet>();

		//<M_PromotionDistribution_ID, DistributionSorting>
		Map<Integer, String> sortingType = new HashMap<Integer, String>();
		OrderLineComparator olComparator = new OrderLineComparator(orderLineIndex);
		//distribute order lines
		for (Map.Entry<Integer, List<Integer>> entry : promotions.entrySet()) {
			Query query = new Query(Env.getCtx(), MTable.get(order.getCtx(), I_M_PromotionDistribution.Table_ID),
					"M_PromotionDistribution.M_Promotion_ID = ? AND M_PromotionDistribution.IsActive = 'Y'", order.get_TrxName());
			query.setParameters(new Object[]{entry.getKey()});
			query.setOrderBy("SeqNo");
			List<MPromotionDistribution> list = query.<MPromotionDistribution>list();

			Query rewardQuery = new Query(Env.getCtx(), MTable.get(order.getCtx(), I_M_PromotionReward.Table_ID),
					"M_PromotionReward.M_Promotion_ID = ? AND M_PromotionReward.IsActive = 'Y'", order.get_TrxName());
			rewardQuery.setParameters(new Object[]{entry.getKey()});
			rewardQuery.setOrderBy("SeqNo");
			List<MPromotionReward> rewardList = rewardQuery.<MPromotionReward>list();

			List<MPromotionLine> promotionLines = new ArrayList<MPromotionLine>();
			for (Integer M_PromotionLine_ID : entry.getValue()) {
				MPromotionLine promotionLine = new MPromotionLine(order.getCtx(), M_PromotionLine_ID, order.get_TrxName());
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
						if (orderAmount.compareTo(promotionLine.getMinimumAmt()) >= 0) {
							orderAmount = orderAmount.subtract(promotionLine.getMinimumAmt());
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
				if(!validateDistributions(list, validPromotionLineIDs, orderLineQty))
					break;
				
				for (MPromotionDistribution pd : list) {
					if (entry.getValue().contains(pd.getM_PromotionLine_ID())) {
						//sort available orderline base on distribution sorting type
						List<Integer> orderLineIdList = new ArrayList<Integer>();
						orderLineIdList.addAll(orderLineQty.keySet());
						if (pd.getDistributionSorting() != null) {
							Comparator<Integer> cmp = olComparator;
							if (pd.getDistributionSorting().equals(MPromotionDistribution.DISTRIBUTIONSORTING_Descending))
								cmp = Collections.reverseOrder(cmp);
							Collections.sort(orderLineIdList, cmp);
						}
						DistributionSet prevSet = distributions.get(pd.getM_PromotionDistribution_ID());
						DistributionSet distributionSet = PromotionRule.calculateDistributionQty(pd, prevSet, validPromotionLineIDs, orderLineQty, orderLineIdList, order.get_TrxName());
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
							for(Map.Entry<Integer, BigDecimal> olMap : distributionSet.orderLines.entrySet()) {
								BigDecimal qty = (BigDecimal) olMap.getValue();
								int C_OrderLine_ID = (Integer) olMap.getKey();
								for (MOrderLine ol : lines) {
									if (ol.getC_OrderLine_ID() == C_OrderLine_ID) {
										totalPrice = totalPrice.add(ol.getPriceActual().multiply(qty));
										break;
									}
								}
								distributionSet.orderLines.put(olMap.getKey(), BigDecimal.ZERO);
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
							// iDempiereConsulting __ 05/09/2018 -- Promotion a livello di Order con o senza l'imponibile
							if(pr.get_ValueAsBoolean("LIT_isCalcWithVAT")) 
								discount = pr.getAmount().divide(Env.ONEHUNDRED).multiply(order.getGrandTotal());
							// iDempiereConsulting __ 05/09/2018 -- END
							addDiscountLine(order, null, discount, Env.ONE, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
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
							Comparator<Integer> cmp = new OrderLineComparator(orderLineIndex);
							if (pr.getDistributionSorting().equals(MPromotionReward.DISTRIBUTIONSORTING_Descending))
								cmp = Collections.reverseOrder(cmp);
							Set<Integer> keySet = distributionSet.orderLines.keySet();
							List<Integer> keyList = new ArrayList<Integer>();
							keyList.addAll(keySet);
							Collections.sort(keyList, cmp);
							Map<Integer, BigDecimal>sortedMap = new LinkedHashMap<Integer, BigDecimal>();
							for(Integer id : keyList) {
								sortedMap.put(id, distributionSet.orderLines.get(id));
							}
							distributionSet.orderLines = sortedMap;
						}


						BigDecimal setBalance = distributionSet.setQty;
						BigDecimal toApply = pr.getQty();
						if (toApply == null || toApply.signum() == 0)
							toApply = BigDecimal.valueOf(-1.0);

						BigDecimal totalPrice  = BigDecimal.ZERO;

						for(Map.Entry<Integer, BigDecimal> olMap : distributionSet.orderLines.entrySet()) {
							BigDecimal qty = olMap.getValue();
							int C_OrderLine_ID = olMap.getKey();
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
								BigDecimal newQty = olMap.getValue();
								newQty = newQty.subtract(qty);
								distributionSet.orderLines.put(olMap.getKey(), newQty);
							}
							for (MOrderLine ol : lines) {
								if (ol.getC_OrderLine_ID() == C_OrderLine_ID) {
									if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_Percentage)) {
										BigDecimal priceActual = ol.getPriceActual();
										BigDecimal discount = priceActual.multiply(pr.getAmount().divide(Env.ONEHUNDRED));
										addDiscountLine(order, ol, discount, qty, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
									} else if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_FlatDiscount)) {
										addDiscountLine(order, ol, pr.getAmount(), Env.ONE, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
									} else if (pr.getRewardType().equals(MPromotionReward.REWARDTYPE_AbsoluteAmount)) {
										BigDecimal priceActual = ol.getPriceActual();
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
								addDiscountLine(order, null, totalPrice.subtract(pr.getAmount()), Env.ONE, pr.getC_Charge_ID(), pr.getM_Promotion(), isVATCalcLine);
							}
						}
					}
				}
			}
			
			// iDempiereConsulting __ 20/09/2018 -- Promotion CONAI ....consolid
			if(/*isConsolid &&*/ listConsolid!=null && !listConsolid.isEmpty()) {
				
				for (Integer t_ID : listTaxID) {
					String sum = entry.getKey().intValue()+"_"+t_ID.intValue();
					List<MOrderLine> listOrders = (listConsolid.get(sum));
					if(listOrders != null) {
						MOrderLine tmp_line = listOrders.get(0);
						BigDecimal priceStd = tmp_line.getPriceEntered();
						BigDecimal priceList = tmp_line.getPriceList();
						Integer proID_CONAI = tmp_line.getM_Product_ID();
						Integer uomID_CONAI = tmp_line.getC_UOM_ID();
						Function<MOrderLine, BigDecimal> totalMapper = orderLine -> orderLine.getQtyEntered();
						BigDecimal tot_qty = listOrders.stream().map(totalMapper).reduce(BigDecimal.ZERO, BigDecimal::add);
						
						MOrderLine nol = new MOrderLine(order.getCtx(), 0, order.get_TrxName());
						nol.setC_Order_ID(order.getC_Order_ID());
						nol.setAD_Org_ID(order.getAD_Org_ID());
						nol.setOrder(order);
						nol.setM_Product_ID(proID_CONAI);
						nol.setC_UOM_ID(uomID_CONAI);
						nol.setQty(tot_qty);
						nol.setC_Tax_ID(tmp_line.getC_Tax_ID());
						nol.setPriceEntered(priceStd);
						nol.setPriceActual(priceStd);
						nol.setPriceList(priceList);
						nol.setLineNetAmt();
		
						nol.setDescription(tmp_line.getProduct().getDescription());
						nol.set_ValueOfColumn("M_Promotion_ID", tmp_line.get_ValueAsInt("M_Promotion_ID"));
						if (tmp_line.getC_Campaign_ID() > 0) {
							nol.setC_Campaign_ID(tmp_line.getC_Campaign_ID());
						}
						if (!nol.save())
							throw new AdempiereException(Msg.getMsg(Env.getCtx(), "LIT_CONAI_order"));
						
					}
				}
			}
			//
		}
		if(listConsolid!=null)
			listConsolid.clear();
		if(listCount_ol!=null)
			listCount_ol.clear();
		if(listTaxID!=null)
			listTaxID.clear();
		// iDempiereConsulting __ 23/05/2018 -- END
	}

	// iDempiereConsulting __ 11/09/2018 -- Promotion CONAI
	private static void addCONAILine(MOrder order, MOrderLine oLine, X_M_ProductPriceVendorBreak prodPriceBreak, MProductPrice prodPrice2, BigDecimal qty,
			MProduct product_CONAI, I_M_Promotion promotion, boolean isVATCalcLine, boolean isConsolid) throws Exception{
		MOrderLine nol = new MOrderLine(order.getCtx(), 0, order.get_TrxName());
		nol.setC_Order_ID(order.getC_Order_ID());
		nol.setOrder(order);
		nol.setM_Product_ID(product_CONAI.getM_Product_ID());
		nol.setC_UOM_ID(product_CONAI.getC_UOM_ID());
		nol.setQty(qty);
		
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
			throw new AdempiereException(Msg.getMsg(Env.getCtx(), "LIT_CONAI_OrderProduct") +" "+product_CONAI.getName());
		
		//iDempiereConsulting __ 09/10/2018 -- Calcolo o no dell'IVA a livello riga promozione/CONAI
		if(isVATCalcLine && oLine != null)
			nol.setC_Tax_ID(oLine.getC_Tax_ID());
		else
			nol.setTax();
		//iDempiereConsulting __ 09/10/2018 -- END
		
		nol.setPriceEntered(priceStd);
		nol.setPriceActual(priceStd);
		nol.setPriceList(priceList);

//		nol.setDiscount();
		nol.setLineNetAmt();

		String description = product_CONAI.getName();

		nol.setDescription(description + " --- " + oLine.getProduct().getValue()+" ___ "+oLine.getProduct().getName());
		nol.set_ValueOfColumn("M_Promotion_ID", promotion.getM_Promotion_ID());
		if (promotion.getC_Campaign_ID() > 0) {
			nol.setC_Campaign_ID(promotion.getC_Campaign_ID());
		}

		if(isConsolid) {
			String sum = promotion.getM_Promotion_ID()+"_"+nol.getC_Tax_ID(); //key per recupero dati di consolida in unica riga per tasse diverse
			if(listConsolid.containsKey(sum)) {
				(listConsolid.get(sum)).add(nol);
			}
			else {
				listCount_ol = new ArrayList<MOrderLine>();
				listCount_ol.add(nol);
				listConsolid.put(sum, listCount_ol);
			}
//			if(listTaxID==null)
//				listTaxID = new ArrayList<Integer>();
			if(!listTaxID.contains(nol.getC_Tax_ID()))
				listTaxID.add(nol.getC_Tax_ID());
		}
		else {
			if (!nol.save())
				throw new AdempiereException(Msg.getMsg(Env.getCtx(), "LIT_CONAI_order"));
			}
	}

	/**
	 * Validate if There are Order Line Qty for this Promotion
	 * @param list of Distributions
	 * @param validPromotionLineIDs 
	 * @param orderLineQty
	 * @return
	 */
	private static boolean validateDistributions(List<MPromotionDistribution> list, List<Integer> validPromotionLineIDs, Map<Integer, BigDecimal> orderLineQty) {
		for(MPromotionDistribution pd : list) {
			if(!pd.getM_PromotionLine().isMandatoryPL())
				continue;
			
			List<Integer> orderLineIdList = new ArrayList<Integer>();
			orderLineIdList.addAll(orderLineQty.keySet());
			List<Integer> eligibleOrderLineIDs = getEligibleOrderLines(pd, validPromotionLineIDs, orderLineIdList, orderLineQty);
			if (eligibleOrderLineIDs.isEmpty())
				return false;

			BigDecimal compareQty = pd.getQty();
			BigDecimal totalOrderLineQty = BigDecimal.ZERO;
			for (int C_OrderLine_ID : eligibleOrderLineIDs) {
				BigDecimal availableQty = orderLineQty.get(C_OrderLine_ID);
				if (availableQty.signum() <= 0) continue;
				totalOrderLineQty = totalOrderLineQty.add(availableQty);
			}
			int compare = totalOrderLineQty.compareTo(compareQty);
			boolean match = false;
			if (compare <= 0 && "<=".equals(pd.getOperation())) {
				match = true;
			} else if (compare >= 0 && ">=".equals(pd.getOperation())) {
				match = true;
			}
			if(!match)
				return false;
		}
		return true;
	}

	/**
	 * Order lines eligible for this distribution
	 * @param distribution
	 * @param validPromotionLineIDs
	 * @param orderLineIdList
	 * @param orderLineQty
	 * @return Eligible Order Lines 
	 */
	private static List<Integer> getEligibleOrderLines(MPromotionDistribution distribution, List<Integer> validPromotionLineIDs, List<Integer> orderLineIdList, Map<Integer, BigDecimal> orderLineQty) {
		
		String sql = "SELECT C_OrderLine.C_OrderLine_ID FROM M_PromotionLine"
				+ " INNER JOIN M_PromotionGroup ON (M_PromotionLine.M_PromotionGroup_ID = M_PromotionGroup.M_PromotionGroup_ID AND M_PromotionGroup.IsActive = 'Y')"
				+ " INNER JOIN M_PromotionGroupLine ON (M_PromotionGroup.M_PromotionGroup_ID = M_PromotionGroupLine.M_PromotionGroup_ID AND M_PromotionGroupLine.IsActive = 'Y')"
				+ " INNER JOIN C_OrderLine ON (M_PromotionGroupLine.M_Product_ID = C_OrderLine.M_Product_ID)"
				+ " WHERE M_PromotionLine.M_PromotionLine_ID = ? AND C_OrderLine.C_OrderLine_ID = ?"
				+ " AND M_PromotionLine.IsActive = 'Y'";

		List<Integer>eligibleOrderLineIDs = new ArrayList<Integer>();
		if (distribution.getM_PromotionLine().getM_PromotionGroup_ID() == 0) {
			if (validPromotionLineIDs.contains(distribution.getM_PromotionLine_ID())) {
				eligibleOrderLineIDs.addAll(orderLineIdList);
			}
		} else {
			for(int C_OrderLine_ID : orderLineIdList) {
				BigDecimal availableQty = orderLineQty.get(C_OrderLine_ID);
				if (availableQty.signum() <= 0) continue;
				PreparedStatement stmt = null;
				ResultSet rs = null;
				try {
					stmt = DB.prepareStatement(sql, distribution.get_TrxName());
					stmt.setInt(1, distribution.getM_PromotionLine_ID());
					stmt.setInt(2, C_OrderLine_ID);
					rs = stmt.executeQuery();
					if (rs.next()) {
						eligibleOrderLineIDs.add(C_OrderLine_ID);
					}
				} catch (Exception e) {
					throw new AdempiereException(e.getLocalizedMessage(), e);
				} finally {
					DB.close(rs, stmt);
				}
			}
		}
		return eligibleOrderLineIDs;
	}

	/**
	 * Add discount line (order line with charge)
	 * @param order
	 * @param ol
	 * @param discount
	 * @param qty
	 * @param C_Charge_ID
	 * @param promotion
	 * @throws Exception
	 */
	private static void addDiscountLine(MOrder order, MOrderLine ol, BigDecimal discount,
			BigDecimal qty, int C_Charge_ID, I_M_Promotion promotion, boolean isVATCalcLine) throws Exception {
		MOrderLine nol = new MOrderLine(order.getCtx(), 0, order.get_TrxName());
		nol.setC_Order_ID(order.getC_Order_ID());
		nol.setOrder(order);
		nol.setC_Charge_ID(C_Charge_ID);
		nol.setQty(qty);
		if (discount.scale() > 2)
			discount = discount.setScale(2, RoundingMode.HALF_UP);
		
		//iDempiereConsulting __ 09/10/2018 -- Calcolo o no dell'IVA a livello riga promozione/CONAI
		if(isVATCalcLine && ol != null) 
			nol.setC_Tax_ID(ol.getC_Tax_ID());
		//iDempiereConsulting __ 09/10/2018 -- END
		
		nol.setPriceEntered(discount.negate());
		nol.setPriceActual(discount.negate());
		if (ol != null && Integer.toString(ol.getLine()).endsWith("0")) {
			for(int i = 0; i < 9; i++) {
				int line = ol.getLine() + i + 1;
				int r = DB.getSQLValue(order.get_TrxName(), "SELECT C_OrderLine_ID FROM C_OrderLine WHERE C_Order_ID = ? AND Line = ?", order.getC_Order_ID(), line);
				if (r <= 0) {
					nol.setLine(line);
					break;
				}
			}
		}
		String description = promotion.getName();
		if (ol != null)
			description += (", " + ol.getName());
		nol.setDescription(description);
		nol.set_ValueOfColumn("M_Promotion_ID", promotion.getM_Promotion_ID());
		if (promotion.getC_Campaign_ID() > 0) {
			nol.setC_Campaign_ID(promotion.getC_Campaign_ID());
		}
		if (!nol.save())
			throw new AdempiereException(Msg.getMsg(Env.getCtx(), "LIT_CONAI_discount"));
	}

	/**
	 * Find applicable promotion rules.
	 * @param order
	 * @return Map<M_Promotion_ID, List<M_PromotionLine_ID>>
	 * @throws Exception
	 */
	private static Map<Integer, List<Integer>> findM_Promotion_ID(MOrder order) throws Exception {
		String select = "SELECT M_Promotion.M_Promotion_ID From M_Promotion Inner Join M_PromotionPreCondition "
			+ " ON (M_Promotion.M_Promotion_ID = M_PromotionPreCondition.M_Promotion_ID)";

		String bpFilter = "M_PromotionPreCondition.C_BPartner_ID = ? OR M_PromotionPreCondition.C_BP_Group_ID = ? OR (M_PromotionPreCondition.C_BPartner_ID IS NULL AND M_PromotionPreCondition.C_BP_Group_ID IS NULL)";
		String priceListFilter = "M_PromotionPreCondition.M_PriceList_ID IS NULL OR M_PromotionPreCondition.M_PriceList_ID = ?";
		String warehouseFilter = "M_PromotionPreCondition.M_Warehouse_ID IS NULL OR M_PromotionPreCondition.M_Warehouse_ID = ?";
		String dateFilter = "M_PromotionPreCondition.StartDate <= ? AND (M_PromotionPreCondition.EndDate >= ? OR M_PromotionPreCondition.EndDate IS NULL)";

		//optional promotion code filter
		String promotionCode = order.getPromotionCode();

		StringBuilder sql = new StringBuilder();
		sql.append(select)
			.append(" WHERE")
			.append(" (" + bpFilter + ")")
			.append(" AND (").append(priceListFilter).append(")")
			.append(" AND (").append(warehouseFilter).append(")")
			.append(" AND (").append(dateFilter).append(")");
		if (promotionCode != null && promotionCode.trim().length() > 0) {
			sql.append(" AND (M_PromotionPreCondition.PromotionCode = ?)");
		} else {
			sql.append(" AND (M_PromotionPreCondition.PromotionCode IS NULL)");
		}
		
		//optional activity filter
		int C_Activity_ID = order.getC_Activity_ID();
		if (C_Activity_ID > 0) {
			sql.append(" AND (M_PromotionPreCondition.C_Activity_ID = ? OR M_PromotionPreCondition.C_Activity_ID IS NULL)");
		} else {
			sql.append(" AND (M_PromotionPreCondition.C_Activity_ID IS NULL)");
		}
		
		sql.append(" AND (M_Promotion.AD_Client_ID in (0, ?))")
			.append(" AND (M_Promotion.AD_Org_ID in (0, ?))")
			.append(" AND (M_Promotion.IsActive = 'Y')")
			.append(" AND (M_PromotionPreCondition.IsActive = 'Y')")
			.append(" AND (M_PromotionPreCondition.LIT_isPromForOrder = 'Y')")
			.append(" AND (M_PromotionPreCondition.isSOTrx = '"+((order.isSOTrx())?"Y":"N")+"')")
			.append(" AND (M_PromotionPreCondition.LIT_isPromForLines = 'N')")
			.append(" ORDER BY M_Promotion.PromotionPriority Desc ");

		PreparedStatement stmt = null;
		ResultSet rs = null;
		//Key = M_Promotion_ID, value = List<M_PromotionLine_ID>
		Map<Integer, List<Integer>> promotions = new LinkedHashMap<Integer, List<Integer>>();
		try {
			int pindex = 1;
			stmt = DB.prepareStatement(sql.toString(), order.get_TrxName());
			stmt.setInt(pindex++, order.getC_BPartner_ID());
			stmt.setInt(pindex++, order.getC_BPartner().getC_BP_Group_ID());
			stmt.setInt(pindex++, order.getM_PriceList_ID());
			stmt.setInt(pindex++, order.getM_Warehouse_ID());
			stmt.setTimestamp(pindex++, order.getDateOrdered());
			stmt.setTimestamp(pindex++, order.getDateOrdered());
			if (promotionCode != null && promotionCode.trim().length() > 0) {
				stmt.setString(pindex++, promotionCode);
			}
			if (C_Activity_ID > 0) {
				stmt.setInt(pindex++, C_Activity_ID);
			}
			stmt.setInt(pindex++, order.getAD_Client_ID());
			stmt.setInt(pindex++, order.getAD_Org_ID());
			rs = stmt.executeQuery();
			while(rs.next()) {
				int M_Promotion_ID = rs.getInt(1);
				List<Integer> promotionLineIDs = findPromotionLine(M_Promotion_ID, order);
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
	private static Map<Integer, List<Integer>> findM_Promotion_IDByOrderLine(MOrderLine oLine) throws Exception {
		MOrder order = new MOrder(Env.getCtx(), oLine.getC_Order_ID(), null);

		String select = "SELECT M_Promotion.M_Promotion_ID From M_Promotion Inner Join M_PromotionPreCondition "
				+ " ON (M_Promotion.M_Promotion_ID = M_PromotionPreCondition.M_Promotion_ID)";

		String bpFilter = "M_PromotionPreCondition.C_BPartner_ID = ? OR M_PromotionPreCondition.C_BP_Group_ID = ? OR (M_PromotionPreCondition.C_BPartner_ID IS NULL AND M_PromotionPreCondition.C_BP_Group_ID IS NULL)";
		String priceListFilter = "M_PromotionPreCondition.M_PriceList_ID IS NULL OR M_PromotionPreCondition.M_PriceList_ID = ?";
		String warehouseFilter = "M_PromotionPreCondition.M_Warehouse_ID IS NULL OR M_PromotionPreCondition.M_Warehouse_ID = ?";
		String dateFilter = "M_PromotionPreCondition.StartDate <= ? AND (M_PromotionPreCondition.EndDate >= ? OR M_PromotionPreCondition.EndDate IS NULL)";

		//optional promotion code filter
		String promotionCode = (String)oLine.get_Value("PromotionCode");

		StringBuilder sql = new StringBuilder();
		sql.append(select)
		.append(" WHERE")
		.append(" (" + bpFilter + ")")
		.append(" AND (").append(priceListFilter).append(")")
		.append(" AND (").append(warehouseFilter).append(")")
		.append(" AND (").append(dateFilter).append(")");
		if (promotionCode != null && promotionCode.trim().length() > 0) {
			sql.append(" AND (M_PromotionPreCondition.PromotionCode = ?)");
		} else {
			sql.append(" AND (M_PromotionPreCondition.PromotionCode IS NULL)");
		}


		//optional activity filter
		int C_Activity_ID = oLine.getC_Activity_ID();
		if (C_Activity_ID > 0) {
			sql.append(" AND (M_PromotionPreCondition.C_Activity_ID = ? OR M_PromotionPreCondition.C_Activity_ID IS NULL)");
		} else {
			sql.append(" AND (M_PromotionPreCondition.C_Activity_ID IS NULL)");
		}

		sql.append(" AND (M_Promotion.AD_Client_ID in (0, ?))")
		.append(" AND (M_Promotion.AD_Org_ID in (0, ?))")
		.append(" AND (M_Promotion.IsActive = 'Y')")
		.append(" AND (M_PromotionPreCondition.IsActive = 'Y')")
		.append(" AND (M_PromotionPreCondition.LIT_isPromForOrder = 'Y')")
		.append(" AND (M_PromotionPreCondition.isSOTrx = '"+((order.isSOTrx())?"Y":"N")+"')")
		.append(" AND (M_PromotionPreCondition.LIT_isPromForLines = 'Y')")
		.append(" ORDER BY M_Promotion.PromotionPriority Desc ");

		PreparedStatement stmt = null;
		ResultSet rs = null;
		//Key = M_Promotion_ID, value = List<M_PromotionLine_ID>
		Map<Integer, List<Integer>> promotions = new LinkedHashMap<Integer, List<Integer>>();
		try {
			int pindex = 1;
			stmt = DB.prepareStatement(sql.toString(), oLine.get_TrxName());
			stmt.setInt(pindex++, oLine.getC_BPartner_ID());
			stmt.setInt(pindex++, oLine.getC_BPartner().getC_BP_Group_ID());
			stmt.setInt(pindex++, oLine.getC_Order().getM_PriceList_ID());
			stmt.setInt(pindex++, oLine.getM_Warehouse_ID());
			stmt.setTimestamp(pindex++, oLine.getC_Order().getDateOrdered());
			stmt.setTimestamp(pindex++, oLine.getC_Order().getDateOrdered());
			if (promotionCode != null && promotionCode.trim().length() > 0) {
				stmt.setString(pindex++, promotionCode);
			}
			if (C_Activity_ID > 0) {
				stmt.setInt(pindex++, C_Activity_ID);
			}
			stmt.setInt(pindex++, oLine.getC_Order().getAD_Client_ID());
			stmt.setInt(pindex++, oLine.getC_Order().getAD_Org_ID());
			rs = stmt.executeQuery();
			while(rs.next()) {
				int M_Promotion_ID = rs.getInt(1);
				List<Integer> promotionLineIDs = findPromotionLine(M_Promotion_ID, order);
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
	 * Calculate distribution quantity for order lines
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
			DistributionSet prevSet, List<Integer> validPromotionLineIDs, Map<Integer, BigDecimal> orderLineQty, List<Integer> orderLineIdList, String trxName) throws Exception {

		DistributionSet distributionSet = new DistributionSet();
		List<Integer> eligibleOrderLineIDs = getEligibleOrderLines(distribution, validPromotionLineIDs, orderLineIdList, orderLineQty);

		if (eligibleOrderLineIDs.isEmpty()) {
			distributionSet.setQty = BigDecimal.ZERO;
			return distributionSet;
		}

		BigDecimal compareQty = distribution.getQty();

		BigDecimal setQty = BigDecimal.ZERO;
		BigDecimal totalOrderLineQty = BigDecimal.ZERO;
		for (int C_OrderLine_ID : eligibleOrderLineIDs) {
			BigDecimal availableQty = orderLineQty.get(C_OrderLine_ID);
			if (availableQty.signum() <= 0) continue;
			totalOrderLineQty = totalOrderLineQty.add(availableQty);
		}
		int compare = totalOrderLineQty.compareTo(compareQty);
		boolean match = false;
		if (compare <= 0 && "<=".equals(distribution.getOperation())) {
			match = true;
		} else if (compare >= 0 && ">=".equals(distribution.getOperation())) {
			match = true;
		}
		if (match) {
			if (MPromotionDistribution.DISTRIBUTIONTYPE_Max.equals(distribution.getDistributionType())) {
				setQty = compare > 0 ? totalOrderLineQty : distribution.getQty();
			} else if (MPromotionDistribution.DISTRIBUTIONTYPE_Min.equals(distribution.getDistributionType())) {
				setQty = compare < 0 ? totalOrderLineQty : distribution.getQty();
			} else {
				setQty = compare > 0 ? totalOrderLineQty.subtract(distribution.getQty())
						: distribution.getQty().subtract(totalOrderLineQty);
			}
			distributionSet.setQty = setQty;
			while (setQty.signum() > 0) {
				if (prevSet != null) {
					BigDecimal recycleQty = BigDecimal.ZERO;
					for(Map.Entry<Integer, BigDecimal> entry : prevSet.orderLines.entrySet()) {
						if (entry.getValue().signum() > 0) {
							setQty = setQty.subtract(entry.getValue());
							distributionSet.orderLines.put(entry.getKey(), entry.getValue());
							recycleQty = recycleQty.add(entry.getValue());
						}
					}
					if (recycleQty.signum() > 0) {
						for (int C_OrderLine_ID : eligibleOrderLineIDs) {
							BigDecimal availableQty = orderLineQty.get(C_OrderLine_ID);
							if (availableQty.signum() <= 0) continue;
							if (availableQty.compareTo(recycleQty) < 0) {
								recycleQty = recycleQty.subtract(availableQty);
								orderLineQty.put(C_OrderLine_ID, BigDecimal.ZERO);
							} else {
								availableQty = availableQty.subtract(recycleQty);
								orderLineQty.put(C_OrderLine_ID, availableQty);
								recycleQty = BigDecimal.ZERO;
							}
							if (recycleQty.signum() <= 0)
								break;
						}
					}
					if (setQty.signum() == 0) break;
				}
				for (int C_OrderLine_ID : eligibleOrderLineIDs) {
					BigDecimal availableQty = orderLineQty.get(C_OrderLine_ID);
					if (availableQty.signum() <= 0) continue;
					if (availableQty.compareTo(setQty) < 0) {
						setQty = setQty.subtract(availableQty);
						distributionSet.orderLines.put(C_OrderLine_ID, availableQty);
						orderLineQty.put(C_OrderLine_ID, BigDecimal.ZERO);
					} else {
						availableQty = availableQty.subtract(setQty);
						distributionSet.orderLines.put(C_OrderLine_ID, setQty);
						orderLineQty.put(C_OrderLine_ID, availableQty);
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
	 * @param promotion_ID
	 * @param order
	 * @return List<M_PromotionLine_ID>
	 * @throws SQLException
	 */
	private static List<Integer> findPromotionLine(int promotion_ID, MOrder order) throws SQLException {
		Query query = new Query(Env.getCtx(), MTable.get(order.getCtx(), I_M_PromotionLine.Table_ID), " M_PromotionLine.M_Promotion_ID = ? AND M_PromotionLine.IsActive = 'Y'", order.get_TrxName());
		query.setParameters(new Object[]{promotion_ID});
		List<MPromotionLine>plist = query.<MPromotionLine>list();
		//List<M_PromotionLine_ID>
		List<Integer>applicable = new ArrayList<Integer>();
		MOrderLine[] lines = order.getLines();
		String sql = "SELECT DISTINCT C_OrderLine.C_OrderLine_ID FROM M_PromotionGroup INNER JOIN M_PromotionGroupLine"
				+ " ON (M_PromotionGroup.M_PromotionGroup_ID = M_PromotionGroupLine.M_PromotionGroup_ID AND M_PromotionGroupLine.IsActive = 'Y')"
				+ " INNER JOIN C_OrderLine ON (M_PromotionGroupLine.M_Product_ID = C_OrderLine.M_Product_ID)"
				+ " INNER JOIN M_PromotionLine ON (M_PromotionLine.M_PromotionGroup_ID = M_PromotionGroup.M_PromotionGroup_ID)"
				+ " WHERE M_PromotionLine.M_PromotionLine_ID = ? AND C_OrderLine.C_Order_ID = ?"
				+ " AND M_PromotionLine.IsActive = 'Y'"
				+ " AND M_PromotionGroup.IsActive = 'Y'";
		for (MPromotionLine pl : plist) {
			boolean match = false;
			if (pl.getM_PromotionGroup_ID() > 0) {
				
				PreparedStatement stmt = null;
				ResultSet rs = null;
				try {
					stmt = DB.prepareStatement(sql, order.get_TrxName());
					stmt.setInt(1, pl.getM_PromotionLine_ID());
					stmt.setInt(2, order.getC_Order_ID());
					rs = stmt.executeQuery();
					BigDecimal orderAmt = BigDecimal.ZERO;
					while(rs.next()) {
						if (pl.getMinimumAmt() != null && pl.getMinimumAmt().signum() > 0) {
							int C_OrderLine_ID = rs.getInt(1);
							for (MOrderLine ol : lines) {
								if (ol.getC_OrderLine_ID() == C_OrderLine_ID) {
									orderAmt = orderAmt.add(ol.getLineNetAmt());
									break;
								}
							}
							if (orderAmt.compareTo(pl.getMinimumAmt()) >= 0) {
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
			} else if (pl.getMinimumAmt() != null && pl.getMinimumAmt().compareTo(order.getGrandTotal()) <= 0 ) {
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

	protected static class DistributionSet {
		//<C_OrderLine_Id, DistributionQty>
		Map<Integer, BigDecimal> orderLines = new LinkedHashMap<Integer, BigDecimal>();
		BigDecimal setQty = BigDecimal.ZERO;
	}

	/**
	 * Price actual comparator for order line
	 */
	protected static class OrderLineComparator implements Comparator<Integer> {
		/** C_OrderLine_ID:MOrderLine */
		protected Map<Integer, MOrderLine> index;
		
		/**
		 * @param olIndex order lines
		 */
		protected OrderLineComparator(Map<Integer, MOrderLine> olIndex) {
			index = olIndex;
		}

		public int compare(Integer ol1, Integer ol2) {
			return index.get(ol1).getPriceActual().compareTo(index.get(ol2).getPriceActual());
		}
	}
}
