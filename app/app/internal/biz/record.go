package biz

import (
	"context"
	v1 "dhb/app/app/api"
	"github.com/go-kratos/kratos/v2/log"
	"strconv"
	"strings"
	"time"
)

type EthUserRecord struct {
	ID        int64
	UserId    int64
	Hash      string
	Status    string
	Type      string
	Amount    string
	RelAmount int64
	CoinType  string
}

type Location struct {
	ID            int64
	UserId        int64
	Status        string
	CurrentLevel  int64
	Current       int64
	CurrentMax    int64
	CurrentMaxNew int64
	Row           int64
	Col           int64
	StopDate      time.Time
	CreatedAt     time.Time
}

type LocationNew struct {
	ID                int64
	UserId            int64
	Term              int64
	Status            string
	Current           int64
	CurrentMax        int64
	CurrentMaxNew     int64
	StopLocationAgain int64
	OutRate           int64
	StopCoin          int64
	StopDate          time.Time
	CreatedAt         time.Time
}

type GlobalLock struct {
	ID     int64
	Status int64
}

type RecordUseCase struct {
	ethUserRecordRepo             EthUserRecordRepo
	userRecommendRepo             UserRecommendRepo
	configRepo                    ConfigRepo
	locationRepo                  LocationRepo
	userBalanceRepo               UserBalanceRepo
	userInfoRepo                  UserInfoRepo
	userCurrentMonthRecommendRepo UserCurrentMonthRecommendRepo
	tx                            Transaction
	log                           *log.Helper
}

type EthUserRecordRepo interface {
	GetEthUserRecordListByHash(ctx context.Context, hash ...string) (map[string]*EthUserRecord, error)
	CreateEthUserRecordListByHash(ctx context.Context, r *EthUserRecord) (*EthUserRecord, error)
}

type LocationRepo interface {
	CreateLocation(ctx context.Context, rel *Location) (*Location, error)
	GetLocationLast(ctx context.Context) (*Location, error)
	GetMyLocationLast(ctx context.Context, userId int64) (*LocationNew, error)
	GetLocationDailyYesterday(ctx context.Context, day int) ([]*LocationNew, error)
	GetMyStopLocationLast(ctx context.Context, userId int64) (*Location, error)
	GetMyLocationRunningLast(ctx context.Context, userId int64) (*Location, error)
	GetLocationsByUserId(ctx context.Context, userId int64) ([]*Location, error)
	GetRewardLocationByRowOrCol(ctx context.Context, row int64, col int64, locationRowConfig int64) ([]*Location, error)
	GetRewardLocationByIds(ctx context.Context, ids ...int64) (map[int64]*Location, error)
	UpdateLocation(ctx context.Context, id int64, status string, current int64, stopDate time.Time) error
	GetLocations(ctx context.Context, b *Pagination, userId int64) ([]*LocationNew, error, int64)
	GetUserBalanceRecords(ctx context.Context, b *Pagination, userId int64, coinType string) ([]*UserBalanceRecord, error, int64)
	GetLocationsAll(ctx context.Context, b *Pagination, userId int64) ([]*LocationNew, error, int64)
	UpdateLocationRowAndCol(ctx context.Context, id int64) error
	GetLocationsStopNotUpdate(ctx context.Context) ([]*Location, error)
	LockGlobalLocation(ctx context.Context) (bool, error)
	UnLockGlobalLocation(ctx context.Context) (bool, error)
	LockGlobalWithdraw(ctx context.Context) (bool, error)
	UnLockGlobalWithdraw(ctx context.Context) (bool, error)
	GetLockGlobalLocation(ctx context.Context) (*GlobalLock, error)
	GetLocationUserCount(ctx context.Context) int64
	GetLocationByIds(ctx context.Context, userIds ...int64) ([]*LocationNew, error)
	GetAllLocations(ctx context.Context) ([]*Location, error)
	GetAllLocationsNew(ctx context.Context) ([]*LocationNew, error)
	GetLocationsByUserIds(ctx context.Context, userIds []int64) ([]*Location, error)

	CreateLocationNew(ctx context.Context, rel *LocationNew, amount int64) (*LocationNew, error)
	GetMyStopLocationsLast(ctx context.Context, userId int64) ([]*LocationNew, error)
	GetLocationsNewByUserId(ctx context.Context, userId int64) ([]*LocationNew, error)
	UpdateLocationNew(ctx context.Context, id int64, status string, current int64, stopDate time.Time) error
	UpdateLocationNewCurrent(ctx context.Context, id int64, current int64) error
	GetRunningLocations(ctx context.Context) ([]*LocationNew, error)
}

func NewRecordUseCase(
	ethUserRecordRepo EthUserRecordRepo,
	locationRepo LocationRepo,
	userBalanceRepo UserBalanceRepo,
	userRecommendRepo UserRecommendRepo,
	userInfoRepo UserInfoRepo,
	configRepo ConfigRepo,
	userCurrentMonthRecommendRepo UserCurrentMonthRecommendRepo,
	tx Transaction,
	logger log.Logger) *RecordUseCase {
	return &RecordUseCase{
		ethUserRecordRepo:             ethUserRecordRepo,
		locationRepo:                  locationRepo,
		configRepo:                    configRepo,
		userRecommendRepo:             userRecommendRepo,
		userBalanceRepo:               userBalanceRepo,
		userCurrentMonthRecommendRepo: userCurrentMonthRecommendRepo,
		userInfoRepo:                  userInfoRepo,
		tx:                            tx,
		log:                           log.NewHelper(logger),
	}
}

func (ruc *RecordUseCase) GetEthUserRecordByTxHash(ctx context.Context, txHash ...string) (map[string]*EthUserRecord, error) {
	return ruc.ethUserRecordRepo.GetEthUserRecordListByHash(ctx, txHash...)
}

func (ruc *RecordUseCase) GetGlobalLock(ctx context.Context) (*GlobalLock, error) {
	return ruc.locationRepo.GetLockGlobalLocation(ctx)
}

func (ruc *RecordUseCase) EthUserRecordHandle(ctx context.Context, ethUserRecord ...*EthUserRecord) (bool, error) {

	var (
		configs []*Config
		term    int64
		//level1Price   int64
		//level2Price   int64
		//level3Price   int64
		//level4Price   int64
		zkfPrice int64
		zkfBase  int64
		//vip1          bool
		//recommendRate int64
	)
	// 配置
	configs, _ = ruc.configRepo.GetConfigByKeys(ctx,
		"term", "level_1_price", "level_2_price", "level_3_price", "level_4_price",
		"zkf_price", "zkf_price_base", "zkf_base", "recommend_rate",
	)
	if nil != configs {
		for _, vConfig := range configs {
			//if "term" == vConfig.KeyName {
			//	//term, _ = strconv.ParseInt(vConfig.Value, 10, 64)
			//} else
			if "zkf_price" == vConfig.KeyName {
				zkfPrice, _ = strconv.ParseInt(vConfig.Value, 10, 64)
			}

			if "zkf_price_base" == vConfig.KeyName {
				zkfBase, _ = strconv.ParseInt(vConfig.Value, 10, 64)
			}
			//else if "level_1_price" == vConfig.KeyName {
			//	level1Price, _ = strconv.ParseInt(vConfig.Value, 10, 64)
			//} else if "level_2_price" == vConfig.KeyName {
			//	level2Price, _ = strconv.ParseInt(vConfig.Value, 10, 64)
			//} else if "level_3_price" == vConfig.KeyName {
			//	level3Price, _ = strconv.ParseInt(vConfig.Value, 10, 64)
			//} else if "level_4_price" == vConfig.KeyName {
			//	level4Price, _ = strconv.ParseInt(vConfig.Value, 10, 64)
			//}
			//else if "zkf_price" == vConfig.KeyName {
			//	zkfPrice, _ = strconv.ParseInt(vConfig.Value, 10, 64)
			//} else if "recommend_rate" == vConfig.KeyName {
			//	recommendRate, _ = strconv.ParseInt(vConfig.Value, 10, 64)
			//}
		}
	}

	//if term < 1 || term > 4 {
	//	return true, nil
	//}

	for _, v := range ethUserRecord {
		var (
			locationCurrent    int64
			locationCurrentMax int64
			currentLocationNew *LocationNew
			//userRecommend           *UserRecommend
			//myUserRecommendUserId   int64
			//myUserRecommendUserInfo *UserInfo
			myLocations []*LocationNew
			//tmpRecommendUserIds     []string
			//myUserInfo              *UserInfo
			err error
		)

		// 获取当前用户的占位信息，已经有运行中的跳过
		myLocations, err = ruc.locationRepo.GetLocationsNewByUserId(ctx, v.UserId)
		if nil == myLocations { // 查询异常跳过本次循环
			continue
		}
		//if 0 < len(myLocations) {
		//	tmpStatusRunning := false
		//	for _, vMyLocations := range myLocations {
		//		if term == vMyLocations.Term {
		//			tmpStatusRunning = true
		//			break
		//		}
		//	}
		//
		//	if tmpStatusRunning { // 有运行中直接跳过本次循环
		//		continue
		//	}
		//}

		//if v.RelAmount >= level1Price*10000000000 && v.RelAmount < level2Price*10000000000 {
		//	locationCurrentMax = level1Price * 10000000 * csdPrice
		//} else if v.RelAmount >= level2Price*10000000000 && v.RelAmount < level3Price*10000000000 {
		//	locationCurrentMax = level2Price * 10000000 * csdPrice
		//} else if v.RelAmount >= level3Price*10000000000 && v.RelAmount < level4Price*10000000000 {
		//	locationCurrentMax = level3Price * 10000000 * csdPrice
		//} else if v.RelAmount >= level4Price*10000000000 {
		//	locationCurrentMax = level4Price * 10000000 * zkfPrice
		//	vip1 = true
		//}

		locationCurrentMax = v.RelAmount * zkfPrice / zkfBase

		// 推荐人
		//userRecommend, err = ruc.userRecommendRepo.GetUserRecommendByUserId(ctx, v.UserId)
		//if nil != err {
		//	continue
		//}
		//var (
		//	tmpRecommendUserIdsInt []int64
		//)
		//if "" != userRecommend.RecommendCode {
		//	tmpRecommendUserIds = strings.Split(userRecommend.RecommendCode, "D")
		//	if 2 <= len(tmpRecommendUserIds) {
		//		myUserRecommendUserId, _ = strconv.ParseInt(tmpRecommendUserIds[len(tmpRecommendUserIds)-1], 10, 64) // 最后一位是直推人
		//	}
		//
		//	lastKey := len(tmpRecommendUserIds) - 1
		//	if 1 <= lastKey {
		//		for i := 0; i <= lastKey; i++ {
		//			// 有占位信息，推荐人推荐人的上一代
		//			if lastKey-i <= 0 {
		//				break
		//			}
		//
		//			tmpMyTopUserRecommendUserId, _ := strconv.ParseInt(tmpRecommendUserIds[lastKey-i], 10, 64) // 最后一位是直推人
		//			tmpRecommendUserIdsInt = append(tmpRecommendUserIdsInt, tmpMyTopUserRecommendUserId)
		//		}
		//	}
		//}

		//if 0 < myUserRecommendUserId {
		//	myUserRecommendUserInfo, err = ruc.userInfoRepo.GetUserInfoByUserId(ctx, myUserRecommendUserId)
		//}

		// 冻结
		//myLastStopLocations, err = ruc.locationRepo.GetMyStopLocationsLast(ctx, v.UserId)
		//now := time.Now().UTC().Add(8 * time.Hour)
		//if nil != myLastStopLocations {
		//	for _, vMyLastStopLocations := range myLastStopLocations {
		//		if now.Before(vMyLastStopLocations.StopDate.Add(time.Duration(timeAgain) * time.Minute)) {
		//			locationCurrent += vMyLastStopLocations.Current - vMyLastStopLocations.CurrentMax // 补上
		//		}
		//	}
		//}
		//myUserInfo, err = ruc.userInfoRepo.GetUserInfoByUserId(ctx, v.UserId)
		//if nil != err {
		//	continue
		//}

		if err = ruc.tx.ExecTx(ctx, func(ctx context.Context) error { // 事务
			tmpLocationStatus := "running"
			var tmpStopDate time.Time
			//if locationCurrent >= locationCurrentMax {
			//	tmpLocationStatus = "stop"
			//	tmpStopDate = time.Now().UTC().Add(8 * time.Hour)
			//}
			currentLocationNew, err = ruc.locationRepo.CreateLocationNew(ctx, &LocationNew{ // 占位
				UserId:     v.UserId,
				Term:       term,
				Status:     tmpLocationStatus,
				Current:    locationCurrent,
				CurrentMax: locationCurrentMax,
				StopDate:   tmpStopDate,
			}, v.RelAmount)
			if nil != err {
				return err
			}

			//if vip1 && 0 == myUserInfo.Vip {
			//	_, err = ruc.userInfoRepo.UpdateUserInfoVip(ctx, v.UserId, 1) // 推荐人信息修改
			//	if nil != err {
			//		return err
			//	}
			//}

			// 推荐人
			//if nil != myUserRecommendUserInfo {
			//	if 0 == len(myLocations) { // vip 等级调整，被推荐人首次入单
			//		myUserRecommendUserInfo.HistoryRecommend += 1
			//		_, err = ruc.userInfoRepo.UpdateUserInfo(ctx, myUserRecommendUserInfo) // 推荐人信息修改
			//		if nil != err {
			//			return err
			//		}
			//	}
			//
			//	// 有占位信息，推荐人第一代
			//	//	myUserRecommendUserLocationsLast, err = ruc.locationRepo.GetLocationsNewByUserId(ctx, myUserRecommendUserInfo.UserId)
			//	//	if nil != myUserRecommendUserLocationsLast {
			//	//		var myUserRecommendUserLocationLast *LocationNew
			//	//		if 1 <= len(myUserRecommendUserLocationsLast) {
			//	//			myUserRecommendUserLocationLast = myUserRecommendUserLocationsLast[0]
			//	//			for _, vMyUserRecommendUserLocationLast := range myUserRecommendUserLocationsLast {
			//	//				if "running" == vMyUserRecommendUserLocationLast.Status {
			//	//					myUserRecommendUserLocationLast = vMyUserRecommendUserLocationLast
			//	//					break
			//	//				}
			//	//			}
			//	//
			//	//			tmpStatus := myUserRecommendUserLocationLast.Status // 现在还在运行中
			//	//
			//	//			// 奖励usdt
			//	//			tmpRewardAmount := currentValue * recommendNeed / 100
			//	//
			//	//			tmpBalanceAmount := tmpRewardAmount * rewardRate / 100 // 记录下一次
			//	//			tmpBalanceCoinAmount := tmpRewardAmount * coinRewardRate / 100 * 1000 / coinPrice
			//	//
			//	//			myUserRecommendUserLocationLast.Status = "running"
			//	//			myUserRecommendUserLocationLast.Current += tmpRewardAmount
			//	//
			//	//			if myUserRecommendUserLocationLast.Current >= myUserRecommendUserLocationLast.CurrentMax { // 占位分红人分满停止
			//	//				myUserRecommendUserLocationLast.Status = "stop"
			//	//				if "running" == tmpStatus {
			//	//					myUserRecommendUserLocationLast.StopDate = time.Now().UTC().Add(8 * time.Hour)
			//	//					// 这里刚刚停止
			//	//					tmpLastAmount := tmpRewardAmount - (myUserRecommendUserLocationLast.Current - myUserRecommendUserLocationLast.CurrentMax)
			//	//					tmpBalanceAmount = tmpLastAmount * rewardRate / 100 // 记录下一次
			//	//					tmpBalanceCoinAmount = tmpLastAmount * coinRewardRate / 100 * 1000 / coinPrice
			//	//				}
			//	//			}
			//	//
			//	//			if 0 < tmpRewardAmount {
			//	//				err = ruc.locationRepo.UpdateLocationNew(ctx, myUserRecommendUserLocationLast.ID, myUserRecommendUserLocationLast.Status, tmpRewardAmount, myUserRecommendUserLocationLast.StopDate) // 分红占位数据修改
			//	//				if nil != err {
			//	//					return err
			//	//				}
			//	//
			//	//				_, err = ruc.userBalanceRepo.NormalRecommendReward(ctx, myUserRecommendUserId, tmpRewardAmount, tmpBalanceAmount, tmpBalanceCoinAmount, currentLocationNew.ID, tmpStatus) // 直推人奖励
			//	//				if nil != err {
			//	//					return err
			//	//				}
			//	//
			//	//			}
			//	//		}
			//	//
			//	//	}
			//
			//	var tmp2 []int64
			//	if 1 < len(tmpRecommendUserIdsInt) {
			//		tmp2 = tmpRecommendUserIdsInt[1 : len(tmpRecommendUserIdsInt)-1]
			//	}
			//
			//	_, err = ruc.userBalanceRepo.NewNormalRecommendReward(ctx, myUserRecommendUserId, locationCurrentMax*recommendRate/1000, currentLocationNew.ID, tmp2) // 直推人奖励
			//	if nil != err {
			//		return err
			//	}
			//}

			// 修改用户推荐人区数据，修改自身区数据
			//_, err = ruc.userRecommendRepo.UpdateUserAreaSelfAmount(ctx, v.UserId, locationCurrentMax/100000)
			//if nil != err {
			//	return err
			//}
			//for _, vTmpRecommendUserIds := range tmpRecommendUserIds {
			//	vTmpRecommendUserId, _ := strconv.ParseInt(vTmpRecommendUserIds, 10, 64)
			//	if vTmpRecommendUserId > 0 {
			//		_, err = ruc.userRecommendRepo.UpdateUserAreaAmount(ctx, vTmpRecommendUserId, locationCurrentMax/100000)
			//		if nil != err {
			//			return err
			//		}
			//	}
			//}

			//_, err = ruc.userBalanceRepo.Deposit(ctx, v.UserId, locationCurrentMax, dhbAmount) // 充值
			//if nil != err {
			//	return err
			//}

			// 清算冻结
			//if nil != myLastStopLocations {
			//	err = ruc.userBalanceRepo.UpdateLocationAgain(ctx, myLastStopLocations) // 充值
			//	if nil != err {
			//		return err
			//	}
			//
			//	if 0 < locationCurrent {
			//		var tmpCurrentAmount int64
			//		if locationCurrent > locationCurrentMax {
			//			tmpCurrentAmount = locationCurrentMax
			//		} else {
			//			tmpCurrentAmount = locationCurrent
			//		}
			//
			//		stopUsdt += tmpCurrentAmount * rewardRate / 100 // 记录下一次
			//		stopCoin += tmpCurrentAmount * coinRewardRate / 100 * 1000 / coinPrice
			//
			//		_, err = ruc.userBalanceRepo.DepositLastNew(ctx, v.UserId, tmpCurrentAmount, stopUsdt, stopCoin) // 充值
			//		if nil != err {
			//			return err
			//		}
			//	}
			//}

			//err = ruc.userBalanceRepo.SystemReward(ctx, amount, currentLocationNew.ID)
			//if nil != err {
			//	return err
			//}

			_, err = ruc.ethUserRecordRepo.CreateEthUserRecordListByHash(ctx, &EthUserRecord{
				Hash:     v.Hash,
				UserId:   v.UserId,
				Status:   v.Status,
				Type:     v.Type,
				Amount:   v.Amount,
				CoinType: v.CoinType,
			})
			if nil != err {
				return err
			}

			return nil
		}); nil != err {
			continue
		}
	}

	return true, nil
}

func (ruc *RecordUseCase) EthUserRecordHandle2(ctx context.Context, ethUserRecord ...*EthUserRecord) (bool, error) {

	var (
	//configs       []*Confi
	//level1Price   int64
	//level2Price   int64
	//level3Price   int64
	//level4Price   int64
	//csdPrice      int64
	//vip1          bool
	//recommendRate int64
	)
	// 配置
	//configs, _ = ruc.configRepo.GetConfigByKeys(ctx,
	//	"term", "level_1_price", "level_2_price", "level_3_price", "level_4_price",
	//	"csd_price", "recommend_rate",
	//)
	//if nil != configs {
	//	for _, vConfig := range configs {
	//		if "term" == vConfig.KeyName {
	//			term, _ = strconv.ParseInt(vConfig.Value, 10, 64)
	//		} else if "level_1_price" == vConfig.KeyName {
	//			level1Price, _ = strconv.ParseInt(vConfig.Value, 10, 64)
	//		} else if "level_2_price" == vConfig.KeyName {
	//			level2Price, _ = strconv.ParseInt(vConfig.Value, 10, 64)
	//		} else if "level_3_price" == vConfig.KeyName {
	//			level3Price, _ = strconv.ParseInt(vConfig.Value, 10, 64)
	//		} else if "level_4_price" == vConfig.KeyName {
	//			level4Price, _ = strconv.ParseInt(vConfig.Value, 10, 64)
	//		} else if "csd_price" == vConfig.KeyName {
	//			csdPrice, _ = strconv.ParseInt(vConfig.Value, 10, 64)
	//		} else if "recommend_rate" == vConfig.KeyName {
	//			recommendRate, _ = strconv.ParseInt(vConfig.Value, 10, 64)
	//		}
	//	}
	//}

	for _, v := range ethUserRecord {
		var (
			err error
		)

		// 获取当前用户的占位信息，已经有运行中的跳过
		//myLocations, err = ruc.locationRepo.GetLocationsNewByUserId(ctx, v.UserId)
		//if nil == myLocations { // 查询异常跳过本次循环
		//	continue
		//}
		//if 0 < len(myLocations) {
		//	tmpStatusRunning := false
		//	for _, vMyLocations := range myLocations {
		//		if term == vMyLocations.Term {
		//			tmpStatusRunning = true
		//			break
		//		}
		//	}
		//
		//	if tmpStatusRunning { // 有运行中直接跳过本次循环
		//		continue
		//	}
		//}

		//if v.RelAmount >= level1Price*10000000000 && v.RelAmount < level2Price*10000000000 {
		//	locationCurrentMax = level1Price * 10000000000 * csdPrice / 1000
		//} else if v.RelAmount >= level2Price*10000000000 && v.RelAmount < level3Price*10000000000 {
		//	locationCurrentMax = level2Price * 10000000000 * csdPrice / 1000
		//} else if v.RelAmount >= level3Price*10000000000 && v.RelAmount < level4Price*10000000000 {
		//	locationCurrentMax = level3Price * 10000000000 * csdPrice / 1000
		//} else if v.RelAmount >= level4Price*10000000000 {
		//	locationCurrentMax = level4Price * 10000000000 * csdPrice / 1000
		//	vip1 = true
		//}

		// 推荐人
		//userRecommend, err = ruc.userRecommendRepo.GetUserRecommendByUserId(ctx, v.UserId)
		//if nil != err {
		//	continue
		//}
		//if "" != userRecommend.RecommendCode {
		//	tmpRecommendUserIds = strings.Split(userRecommend.RecommendCode, "D")
		//	if 2 <= len(tmpRecommendUserIds) {
		//		myUserRecommendUserId, _ = strconv.ParseInt(tmpRecommendUserIds[len(tmpRecommendUserIds)-1], 10, 64) // 最后一位是直推人
		//	}
		//}
		//if 0 < myUserRecommendUserId {
		//	myUserRecommendUserInfo, err = ruc.userInfoRepo.GetUserInfoByUserId(ctx, myUserRecommendUserId)
		//}

		// 冻结
		//myLastStopLocations, err = ruc.locationRepo.GetMyStopLocationsLast(ctx, v.UserId)
		//now := time.Now().UTC().Add(8 * time.Hour)
		//if nil != myLastStopLocations {
		//	for _, vMyLastStopLocations := range myLastStopLocations {
		//		if now.Before(vMyLastStopLocations.StopDate.Add(time.Duration(timeAgain) * time.Minute)) {
		//			locationCurrent += vMyLastStopLocations.Current - vMyLastStopLocations.CurrentMax // 补上
		//		}
		//	}
		//}
		//myUserInfo, err = ruc.userInfoRepo.GetUserInfoByUserId(ctx, v.UserId)
		//if nil != err {
		//	continue
		//}
		//
		if err = ruc.tx.ExecTx(ctx, func(ctx context.Context) error { // 事务
			//	tmpLocationStatus := "running"
			//	var tmpStopDate time.Time
			//	//if locationCurrent >= locationCurrentMax {
			//	//	tmpLocationStatus = "stop"
			//	//	tmpStopDate = time.Now().UTC().Add(8 * time.Hour)
			//	//}
			//	currentLocationNew, err = ruc.locationRepo.CreateLocationNew(ctx, &LocationNew{ // 占位
			//		UserId:     v.UserId,
			//		Term:       term,
			//		Status:     tmpLocationStatus,
			//		Current:    locationCurrent,
			//		CurrentMax: locationCurrentMax,
			//		StopDate:   tmpStopDate,
			//	})
			//	if nil != err {
			//		return err
			//	}
			//
			//	if vip1 && 0 == myUserInfo.Vip {
			//		_, err = ruc.userInfoRepo.UpdateUserInfoVip(ctx, v.UserId, 1) // 推荐人信息修改
			//		if nil != err {
			//			return err
			//		}
			//	}
			//
			//	// 推荐人
			//	if nil != myUserRecommendUserInfo {
			//		if 0 == len(myLocations) { // vip 等级调整，被推荐人首次入单
			//			myUserRecommendUserInfo.HistoryRecommend += 1
			//			_, err = ruc.userInfoRepo.UpdateUserInfo(ctx, myUserRecommendUserInfo) // 推荐人信息修改
			//			if nil != err {
			//				return err
			//			}
			//		}
			//
			//		// 有占位信息，推荐人第一代
			//		//	myUserRecommendUserLocationsLast, err = ruc.locationRepo.GetLocationsNewByUserId(ctx, myUserRecommendUserInfo.UserId)
			//		//	if nil != myUserRecommendUserLocationsLast {
			//		//		var myUserRecommendUserLocationLast *LocationNew
			//		//		if 1 <= len(myUserRecommendUserLocationsLast) {
			//		//			myUserRecommendUserLocationLast = myUserRecommendUserLocationsLast[0]
			//		//			for _, vMyUserRecommendUserLocationLast := range myUserRecommendUserLocationsLast {
			//		//				if "running" == vMyUserRecommendUserLocationLast.Status {
			//		//					myUserRecommendUserLocationLast = vMyUserRecommendUserLocationLast
			//		//					break
			//		//				}
			//		//			}
			//		//
			//		//			tmpStatus := myUserRecommendUserLocationLast.Status // 现在还在运行中
			//		//
			//		//			// 奖励usdt
			//		//			tmpRewardAmount := currentValue * recommendNeed / 100
			//		//
			//		//			tmpBalanceAmount := tmpRewardAmount * rewardRate / 100 // 记录下一次
			//		//			tmpBalanceCoinAmount := tmpRewardAmount * coinRewardRate / 100 * 1000 / coinPrice
			//		//
			//		//			myUserRecommendUserLocationLast.Status = "running"
			//		//			myUserRecommendUserLocationLast.Current += tmpRewardAmount
			//		//
			//		//			if myUserRecommendUserLocationLast.Current >= myUserRecommendUserLocationLast.CurrentMax { // 占位分红人分满停止
			//		//				myUserRecommendUserLocationLast.Status = "stop"
			//		//				if "running" == tmpStatus {
			//		//					myUserRecommendUserLocationLast.StopDate = time.Now().UTC().Add(8 * time.Hour)
			//		//					// 这里刚刚停止
			//		//					tmpLastAmount := tmpRewardAmount - (myUserRecommendUserLocationLast.Current - myUserRecommendUserLocationLast.CurrentMax)
			//		//					tmpBalanceAmount = tmpLastAmount * rewardRate / 100 // 记录下一次
			//		//					tmpBalanceCoinAmount = tmpLastAmount * coinRewardRate / 100 * 1000 / coinPrice
			//		//				}
			//		//			}
			//		//
			//		//			if 0 < tmpRewardAmount {
			//		//				err = ruc.locationRepo.UpdateLocationNew(ctx, myUserRecommendUserLocationLast.ID, myUserRecommendUserLocationLast.Status, tmpRewardAmount, myUserRecommendUserLocationLast.StopDate) // 分红占位数据修改
			//		//				if nil != err {
			//		//					return err
			//		//				}
			//		//
			//		//				_, err = ruc.userBalanceRepo.NormalRecommendReward(ctx, myUserRecommendUserId, tmpRewardAmount, tmpBalanceAmount, tmpBalanceCoinAmount, currentLocationNew.ID, tmpStatus) // 直推人奖励
			//		//				if nil != err {
			//		//					return err
			//		//				}
			//		//
			//		//			}
			//		//		}
			//		//
			//		//	}
			//
			//		_, err = ruc.userBalanceRepo.NewNormalRecommendReward(ctx, myUserRecommendUserId, locationCurrentMax*recommendRate/1000, currentLocationNew.ID) // 直推人奖励
			//		if nil != err {
			//			return err
			//		}
			//	}

			// 修改用户推荐人区数据，修改自身区数据
			//_, err = ruc.userRecommendRepo.UpdateUserAreaSelfAmount(ctx, v.UserId, locationCurrentMax/100000)
			//if nil != err {
			//	return err
			//}
			//for _, vTmpRecommendUserIds := range tmpRecommendUserIds {
			//	vTmpRecommendUserId, _ := strconv.ParseInt(vTmpRecommendUserIds, 10, 64)
			//	if vTmpRecommendUserId > 0 {
			//		_, err = ruc.userRecommendRepo.UpdateUserAreaAmount(ctx, vTmpRecommendUserId, locationCurrentMax/100000)
			//		if nil != err {
			//			return err
			//		}
			//	}
			//}

			//_, err = ruc.userBalanceRepo.Deposit(ctx, v.UserId, locationCurrentMax, dhbAmount) // 充值
			//if nil != err {
			//	return err
			//}

			// 清算冻结
			//if nil != myLastStopLocations {
			//	err = ruc.userBalanceRepo.UpdateLocationAgain(ctx, myLastStopLocations) // 充值
			//	if nil != err {
			//		return err
			//	}
			//
			//	if 0 < locationCurrent {
			//		var tmpCurrentAmount int64
			//		if locationCurrent > locationCurrentMax {
			//			tmpCurrentAmount = locationCurrentMax
			//		} else {
			//			tmpCurrentAmount = locationCurrent
			//		}
			//
			//		stopUsdt += tmpCurrentAmount * rewardRate / 100 // 记录下一次
			//		stopCoin += tmpCurrentAmount * coinRewardRate / 100 * 1000 / coinPrice
			//
			if "CSD" == v.CoinType {
				// 推荐人
				var (
					tmpRecommendUserIds    []string
					tmpRecommendUserIdsInt []int64
				)
				var userRecommend *UserRecommend
				userRecommend, err = ruc.userRecommendRepo.GetUserRecommendByUserId(ctx, v.UserId)
				if nil == err {
					if "" != userRecommend.RecommendCode {
						tmpRecommendUserIds = strings.Split(userRecommend.RecommendCode, "D")
						lastKey := len(tmpRecommendUserIds) - 1
						if 1 <= lastKey {
							for i := 0; i <= lastKey; i++ {
								// 有占位信息，推荐人推荐人的上一代
								if lastKey-i <= 0 {
									break
								}

								tmpMyTopUserRecommendUserId, _ := strconv.ParseInt(tmpRecommendUserIds[lastKey-i], 10, 64) // 最后一位是直推人
								tmpRecommendUserIdsInt = append(tmpRecommendUserIdsInt, tmpMyTopUserRecommendUserId)
							}
						}
					}
				}

				err = ruc.userBalanceRepo.DepositLastNewCsd(ctx, v.UserId, v.RelAmount, tmpRecommendUserIdsInt) // 充值
				if nil != err {
					return err
				}
			} else {
				err = ruc.userBalanceRepo.DepositLastNewDhb(ctx, v.UserId, v.RelAmount) // 充值
				if nil != err {
					return err
				}
			}
			//	}
			//}

			//err = ruc.userBalanceRepo.SystemReward(ctx, amount, currentLocationNew.ID)
			//if nil != err {
			//	return err
			//}

			_, err = ruc.ethUserRecordRepo.CreateEthUserRecordListByHash(ctx, &EthUserRecord{
				Hash:     v.Hash,
				UserId:   v.UserId,
				Status:   v.Status,
				Type:     v.Type,
				Amount:   v.Amount,
				CoinType: v.CoinType,
			})
			if nil != err {
				return err
			}

			return nil
		}); nil != err {
			continue
		}
	}

	return true, nil
}

func (ruc *RecordUseCase) AdminLocationInsert(ctx context.Context, userId int64, amount int64) (bool, error) {

	//var (
	//	currentLocation         *LocationNew
	//	myLastStopLocations     []*LocationNew
	//	stopCoin                int64
	//	stopUsdt                int64
	//	err                     error
	//	configs                 []*Config
	//	myLocations             []*LocationNew
	//	userRecommend           *UserRecommend
	//	tmpRecommendUserIds     []string
	//	myUserRecommendUserInfo *UserInfo
	//	myUserRecommendUserId   int64
	//	locationCurrent         int64
	//	coinPrice               int64
	//	coinRewardRate          int64
	//	rewardRate              int64
	//	outRate                 int64
	//	timeAgain               int64
	//)
	//// 配置
	//configs, _ = ruc.configRepo.GetConfigByKeys(ctx, "recommend_need", "time_again", "out_rate", "coin_price", "reward_rate", "coin_reward_rate")
	//if nil != configs {
	//	for _, vConfig := range configs {
	//		if "time_again" == vConfig.KeyName {
	//			timeAgain, _ = strconv.ParseInt(vConfig.Value, 10, 64)
	//		} else if "out_rate" == vConfig.KeyName {
	//			outRate, _ = strconv.ParseInt(vConfig.Value, 10, 64)
	//		} else if "coin_price" == vConfig.KeyName {
	//			coinPrice, _ = strconv.ParseInt(vConfig.Value, 10, 64)
	//		} else if "coin_reward_rate" == vConfig.KeyName {
	//			coinRewardRate, _ = strconv.ParseInt(vConfig.Value, 10, 64)
	//		} else if "reward_rate" == vConfig.KeyName {
	//			rewardRate, _ = strconv.ParseInt(vConfig.Value, 10, 64)
	//		}
	//	}
	//}
	//
	//// 获取当前用户的占位信息，已经有运行中的跳过
	//myLocations, err = ruc.locationRepo.GetLocationsNewByUserId(ctx, userId)
	//if nil == myLocations { // 查询异常跳过本次循环
	//	return false, errors.New(500, "ERROR", "输入金额错误，重试")
	//}
	//if 0 < len(myLocations) { // 也代表复投
	//	tmpStatusRunning := false
	//	for _, vMyLocations := range myLocations {
	//		if "running" == vMyLocations.Status {
	//			tmpStatusRunning = true
	//			break
	//		}
	//	}
	//
	//	if tmpStatusRunning { // 有运行中直接跳过本次循环
	//		return false, errors.New(500, "ERROR", "已存在运行中位置信息")
	//	}
	//}
	//
	//// 冻结
	//myLastStopLocations, err = ruc.locationRepo.GetMyStopLocationsLast(ctx, userId)
	//now := time.Now().UTC().Add(8 * time.Hour)
	//if nil != myLastStopLocations {
	//	for _, vMyLastStopLocations := range myLastStopLocations {
	//		if now.Before(vMyLastStopLocations.StopDate.Add(time.Duration(timeAgain) * time.Minute)) {
	//			locationCurrent += vMyLastStopLocations.Current - vMyLastStopLocations.CurrentMax // 补上
	//		}
	//	}
	//}
	//
	//// 推荐人
	//userRecommend, err = ruc.userRecommendRepo.GetUserRecommendByUserId(ctx, userId)
	//if nil != err {
	//	return false, errors.New(500, "ERROR", "输入金额错误，重试")
	//}
	//if "" != userRecommend.RecommendCode {
	//	tmpRecommendUserIds = strings.Split(userRecommend.RecommendCode, "D")
	//	if 2 <= len(tmpRecommendUserIds) {
	//		myUserRecommendUserId, _ = strconv.ParseInt(tmpRecommendUserIds[len(tmpRecommendUserIds)-1], 10, 64) // 最后一位是直推人
	//	}
	//}
	//
	//if 0 < myUserRecommendUserId {
	//	myUserRecommendUserInfo, err = ruc.userInfoRepo.GetUserInfoByUserId(ctx, myUserRecommendUserId)
	//}
	//// 推荐人
	//if nil != myUserRecommendUserInfo {
	//	if 0 == len(myLocations) { // vip 等级调整，被推荐人首次入单
	//		myUserRecommendUserInfo.HistoryRecommend += 1
	//		if myUserRecommendUserInfo.HistoryRecommend >= 10 {
	//			myUserRecommendUserInfo.Vip = 5
	//		} else if myUserRecommendUserInfo.HistoryRecommend >= 8 {
	//			myUserRecommendUserInfo.Vip = 4
	//		} else if myUserRecommendUserInfo.HistoryRecommend >= 6 {
	//			myUserRecommendUserInfo.Vip = 3
	//		} else if myUserRecommendUserInfo.HistoryRecommend >= 4 {
	//			myUserRecommendUserInfo.Vip = 2
	//		} else if myUserRecommendUserInfo.HistoryRecommend >= 2 {
	//			myUserRecommendUserInfo.Vip = 1
	//		}
	//	}
	//}
	//
	//if err = ruc.tx.ExecTx(ctx, func(ctx context.Context) error { // 事务
	//	tmpLocationStatus := "running"
	//	var tmpStopDate time.Time
	//	if locationCurrent >= amount*10000000000*outRate {
	//		tmpLocationStatus = "stop"
	//		tmpStopDate = time.Now().UTC().Add(8 * time.Hour)
	//	}
	//
	//	currentLocation, err = ruc.locationRepo.CreateLocationNew(ctx, &LocationNew{ // 占位
	//		UserId:     userId,
	//		Status:     tmpLocationStatus,
	//		Current:    locationCurrent,
	//		OutRate:    outRate,
	//		StopDate:   tmpStopDate,
	//		CurrentMax: amount * 10000000000 * outRate,
	//	})
	//	if nil != err {
	//		return err
	//	}
	//
	//	_, err = ruc.userInfoRepo.UpdateUserInfo(ctx, myUserRecommendUserInfo) // 推荐人信息修改
	//	if nil != err {
	//		return err
	//	}
	//
	//	_, err = ruc.userCurrentMonthRecommendRepo.CreateUserCurrentMonthRecommend(ctx, &UserCurrentMonthRecommend{ // 直推人本月推荐人数
	//		UserId:          myUserRecommendUserId,
	//		RecommendUserId: userId,
	//		Date:            time.Now().UTC().Add(8 * time.Hour),
	//	})
	//	if nil != err {
	//		return err
	//	}
	//
	//	// 清算冻结
	//	if nil != myLastStopLocations {
	//		err = ruc.userBalanceRepo.UpdateLocationAgain(ctx, myLastStopLocations) // 充值
	//		if nil != err {
	//			return err
	//		}
	//		if 0 < locationCurrent {
	//			var tmpCurrentAmount int64
	//			if locationCurrent > amount*10000000000*outRate {
	//				tmpCurrentAmount = amount * 10000000000 * outRate
	//			} else {
	//				tmpCurrentAmount = locationCurrent
	//			}
	//
	//			stopUsdt += tmpCurrentAmount * rewardRate / 100 // 记录下一次
	//			stopCoin += tmpCurrentAmount * coinRewardRate / 100 * 1000 / coinPrice
	//
	//			_, err = ruc.userBalanceRepo.DepositLastNew(ctx, userId, tmpCurrentAmount, stopUsdt, stopCoin) // 充值
	//			if nil != err {
	//				return err
	//			}
	//		}
	//	}
	//
	//	// 修改用户推荐人区数据，修改自身区数据
	//	_, err = ruc.userRecommendRepo.UpdateUserAreaSelfAmount(ctx, userId, amount*100000)
	//	if nil != err {
	//		return err
	//	}
	//	for _, vTmpRecommendUserIds := range tmpRecommendUserIds {
	//		vTmpRecommendUserId, _ := strconv.ParseInt(vTmpRecommendUserIds, 10, 64)
	//		if vTmpRecommendUserId > 0 {
	//			_, err = ruc.userRecommendRepo.UpdateUserAreaAmount(ctx, vTmpRecommendUserId, amount*100000)
	//			if nil != err {
	//				return err
	//			}
	//		}
	//	}
	//
	//	return nil
	//}); nil != err {
	//	return false, errors.New(500, "ERROR", "错误，重试")
	//
	//}

	return true, nil
}

func (ruc *RecordUseCase) LockSystem(ctx context.Context, req *v1.LockSystemRequest) (*v1.LockSystemReply, error) {
	_, _ = ruc.locationRepo.LockGlobalLocation(ctx)
	return nil, nil
}

func (ruc *RecordUseCase) UnLockEthUserRecordHandle(ctx context.Context, ethUserRecord ...*EthUserRecord) (bool, error) {
	return ruc.locationRepo.UnLockGlobalLocation(ctx)
}
