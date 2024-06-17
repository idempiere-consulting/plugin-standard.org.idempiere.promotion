/***********************************************************************
 * This file is part of iDempiere ERP Open Source                      *
 * http://www.idempiere.org                                            *
 *                                                                     *
 * Copyright (C) Contributors                                          *
 *                                                                     *
 * This program is free software; you can redistribute it and/or       *
 * modify it under the terms of the GNU General Public License         *
 * as published by the Free Software Foundation; either version 2      *
 * of the License, or (at your option) any later version.              *
 *                                                                     *
 * This program is distributed in the hope that it will be useful,     *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of      *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the        *
 * GNU General Public License for more details.                        *
 *                                                                     *
 * You should have received a copy of the GNU General Public License   *
 * along with this program; if not, write to the Free Software         *
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,          *
 * MA 02110-1301, USA.                                                 *
 *                                                                     *
 * Contributors:                                                       *
 * - Carlos Ruiz - globalqss - bxservice                               *
 **********************************************************************/

/**
 *
 * @author Carlos Ruiz - globalqss - bxservice
 *
 */
package org.idempiere.model;

import java.sql.ResultSet;

import org.adempiere.base.IModelFactory;
import org.compiere.model.PO;
import org.compiere.util.Env;

public class PromotionModelFactory implements IModelFactory {

	@Override
	public Class<?> getClass(String tableName) {
		if (MPromotion.Table_Name.equals(tableName))
			return MPromotion.class;
		if (MPromotionDistribution.Table_Name.equals(tableName))
			return MPromotionDistribution.class;
		if (MPromotionGroup.Table_Name.equals(tableName))
			return MPromotionGroup.class;
		if (MPromotionGroupLine.Table_Name.equals(tableName))
			return MPromotionGroupLine.class;
		if (MPromotionLine.Table_Name.equals(tableName))
			return MPromotionLine.class;
		if (MPromotionPreCondition.Table_Name.equals(tableName))
			return MPromotionPreCondition.class;
		if (MPromotionReward.Table_Name.equals(tableName))
			return MPromotionReward.class;
		return null;
	}

	@Override
	public PO getPO(String tableName, int Record_ID, String trxName) {
		if (MPromotion.Table_Name.equals(tableName))
			return new MPromotion(Env.getCtx(), Record_ID, trxName);
		if (MPromotionDistribution.Table_Name.equals(tableName))
			return new MPromotionDistribution(Env.getCtx(), Record_ID, trxName);
		if (MPromotionGroup.Table_Name.equals(tableName))
			return new MPromotionGroup(Env.getCtx(), Record_ID, trxName);
		if (MPromotionGroupLine.Table_Name.equals(tableName))
			return new MPromotionGroupLine(Env.getCtx(), Record_ID, trxName);
		if (MPromotionLine.Table_Name.equals(tableName))
			return new MPromotionLine(Env.getCtx(), Record_ID, trxName);
		if (MPromotionPreCondition.Table_Name.equals(tableName))
			return new MPromotionPreCondition(Env.getCtx(), Record_ID, trxName);
		if (MPromotionReward.Table_Name.equals(tableName))
			return new MPromotionReward(Env.getCtx(), Record_ID, trxName);
		return null;
	}

	@Override
	public PO getPO(String tableName, ResultSet rs, String trxName) {
		if (MPromotion.Table_Name.equals(tableName))
			return new MPromotion(Env.getCtx(), rs, trxName);
		if (MPromotionDistribution.Table_Name.equals(tableName))
			return new MPromotionDistribution(Env.getCtx(), rs, trxName);
		if (MPromotionGroup.Table_Name.equals(tableName))
			return new MPromotionGroup(Env.getCtx(), rs, trxName);
		if (MPromotionGroupLine.Table_Name.equals(tableName))
			return new MPromotionGroupLine(Env.getCtx(), rs, trxName);
		if (MPromotionLine.Table_Name.equals(tableName))
			return new MPromotionLine(Env.getCtx(), rs, trxName);
		if (MPromotionPreCondition.Table_Name.equals(tableName))
			return new MPromotionPreCondition(Env.getCtx(), rs, trxName);
		if (MPromotionReward.Table_Name.equals(tableName))
			return new MPromotionReward(Env.getCtx(), rs, trxName);
		return null;
	}

}
