package service

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/sha3"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/auth/jwt"
	jwt2 "github.com/golang-jwt/jwt/v4"
	"io"
	"math/big"
	"net/url"
	"strconv"

	v1 "dhb/app/app/api"
	"dhb/app/app/internal/biz"
	"dhb/app/app/internal/conf"
	"io/ioutil"
	"net/http"
	"time"
)

// AppService service.
type AppService struct {
	v1.UnimplementedAppServer

	uuc *biz.UserUseCase
	ruc *biz.RecordUseCase
	log *log.Helper
	ca  *conf.Auth
}

// NewAppService new a service.
func NewAppService(uuc *biz.UserUseCase, ruc *biz.RecordUseCase, logger log.Logger, ca *conf.Auth) *AppService {
	return &AppService{uuc: uuc, ruc: ruc, log: log.NewHelper(logger), ca: ca}
}

// EthAuthorize ethAuthorize.
func (a *AppService) EthAuthorize(ctx context.Context, req *v1.EthAuthorizeRequest) (*v1.EthAuthorizeReply, error) {
	return nil, nil
}

// Deposit deposit.
func (a *AppService) Deposit(ctx context.Context, req *v1.DepositRequest) (*v1.DepositReply, error) {

	var (
		depositUsdtResult     map[string]*eth
		notExistDepositResult []*biz.EthUserRecord
		existEthUserRecords   map[string]*biz.EthUserRecord
		depositUsers          map[string]*biz.User
		fromAccount           []string
		hashKeys              []string
		err                   error
	)

	end := time.Now().UTC().Add(20 * time.Second)

	// 配置
	//configs, err = a.uuc.GetDhbConfig(ctx)
	//if nil != configs {
	//	for _, vConfig := range configs {
	//		if "level1Dhb" == vConfig.KeyName {
	//			level1Dhb = vConfig.Value + "0000000000000000"
	//		} else if "level2Dhb" == vConfig.KeyName {
	//			level2Dhb = vConfig.Value + "0000000000000000"
	//		} else if "level3Dhb" == vConfig.KeyName {
	//			level3Dhb = vConfig.Value + "0000000000000000"
	//		}
	//	}
	//}

	for i := 1; i <= 5; i++ {
		depositUsdtResult, err = requestEthDepositResult(200, int64(i),
			//"0x55d398326f99059fF775485246999027B3197955",
			"0x337610d27c682E347C9cD60BD4b3b107C9d34dDd",
			"0xe865f2e5ff04b8b7952d1c0d9163a91f313b158f",
		)
		if nil != err {
			break
		}

		//depositUsdtResult2, err = requestEthDepositResult(200, int64(i),
		//	"0x55d398326f99059fF775485246999027B3197955",
		//	"0x983a6385bbac74476d538ad6961920925b617335",
		//)
		//if nil != err {
		//	break
		//}

		now := time.Now().UTC()
		fmt.Println(now, end)
		if end.Before(now) {
			break
		}

		if 0 >= len(depositUsdtResult) {
			break
		}

		//if 0 >= len(depositUsdtResult2) {
		//	break
		//}

		for hashKey, vDepositResult := range depositUsdtResult { // 主查询

			hashKeys = append(hashKeys, hashKey)
			fromAccount = append(fromAccount, vDepositResult.From)
		}

		//for hashKey2, vDepositResult2 := range depositUsdtResult2 { // 主查询
		//	hashKeys = append(hashKeys, hashKey2)
		//	fromAccount = append(fromAccount, vDepositResult2.From)
		//}

		depositUsers, err = a.uuc.GetUserByAddress(ctx, fromAccount...)
		if nil != depositUsers {
			existEthUserRecords, err = a.ruc.GetEthUserRecordByTxHash(ctx, hashKeys...)
			// 统计开始
			notExistDepositResult = make([]*biz.EthUserRecord, 0)
			for _, vDepositUsdtResult := range depositUsdtResult { // 主查usdt
				if _, ok := existEthUserRecords[vDepositUsdtResult.Hash]; ok { // 记录已存在
					continue
				}
				if _, ok := depositUsers[vDepositUsdtResult.From]; !ok { // 用户不存在
					continue
				}

				//var tmp *eth
				//for _, vDepositUsdtResult2 := range depositUsdtResult2 {
				//	if _, ok := existEthUserRecords[vDepositUsdtResult2.Hash]; ok { // 记录已存在
				//		continue
				//	}
				//	if _, ok := depositUsers[vDepositUsdtResult2.From]; !ok { // 用户不存在
				//		continue
				//	}
				//
				//	if vDepositUsdtResult2.From == vDepositUsdtResult.From && vDepositUsdtResult2.Value == vDepositUsdtResult.Value {
				//		tmp = vDepositUsdtResult2
				//	}
				//}

				lenValue := len(vDepositUsdtResult.Value)
				if 18 > lenValue {
					continue
				}

				// 去掉8个尾数0作为系统金额
				tmpValue, _ := strconv.ParseInt(vDepositUsdtResult.Value[0:lenValue-8], 10, 64)
				if 0 == tmpValue {
					continue
				}

				//fmt.Println(vDepositUsdtResult.Value, tmpValue)
				if int64(10000000000) > tmpValue { // 1000000000000
					continue
				}

				//tmpValue = tmpValue / 300 * 1000

				notExistDepositResult = append(notExistDepositResult, &biz.EthUserRecord{ // 两种币的记录
					UserId:    depositUsers[vDepositUsdtResult.From].ID,
					Hash:      vDepositUsdtResult.Hash,
					Status:    "success",
					Type:      "deposit",
					Amount:    strconv.FormatInt(tmpValue, 10) + "00000000",
					RelAmount: tmpValue,
					CoinType:  "USDT",
				})
			}

			_, err = a.ruc.EthUserRecordHandle(ctx, notExistDepositResult...)
			if nil != err {
				fmt.Println(err)
			}
		}
	}

	return &v1.DepositReply{}, nil
}

// Deposit4 deposit.
func (a *AppService) Deposit4(ctx context.Context, req *v1.DepositRequest) (*v1.DepositReply, error) {

	var (
		depositUsdtResult     map[string]*eth
		notExistDepositResult []*biz.EthUserRecord
		existEthUserRecords   map[string]*biz.EthUserRecord
		depositUsers          map[string]*biz.User
		fromAccount           []string
		hashKeys              []string
		err                   error
	)

	time.Sleep(30 * time.Second)
	end := time.Now().UTC().Add(20 * time.Second)

	// 配置
	//configs, err = a.uuc.GetDhbConfig(ctx)
	//if nil != configs {
	//	for _, vConfig := range configs {
	//		if "level1Dhb" == vConfig.KeyName {
	//			level1Dhb = vConfig.Value + "0000000000000000"
	//		} else if "level2Dhb" == vConfig.KeyName {
	//			level2Dhb = vConfig.Value + "0000000000000000"
	//		} else if "level3Dhb" == vConfig.KeyName {
	//			level3Dhb = vConfig.Value + "0000000000000000"
	//		}
	//	}
	//}

	for i := 1; i <= 5; i++ {
		// 0x337610d27c682E347C9cD60BD4b3b107C9d34dDd
		depositUsdtResult, err = requestEthDepositResult(200, int64(i),
			"0xfad476cd33ed9213ed0a2f4c20f6865a98bf0a8b", "0x89c2fa5e5518870fd1fc1f6a1f33cd557c07d3bb")
		if nil != err {
			break
		}

		now := time.Now().UTC()
		fmt.Println(now, end)
		if end.Before(now) {
			break
		}

		if 0 >= len(depositUsdtResult) {
			break
		}

		for hashKey, vDepositResult := range depositUsdtResult { // 主查询
			hashKeys = append(hashKeys, hashKey)
			fromAccount = append(fromAccount, vDepositResult.From)
		}

		depositUsers, err = a.uuc.GetUserByAddress(ctx, fromAccount...)
		if nil != depositUsers {
			existEthUserRecords, err = a.ruc.GetEthUserRecordByTxHash(ctx, hashKeys...)
			// 统计开始
			notExistDepositResult = make([]*biz.EthUserRecord, 0)
			for _, vDepositUsdtResult := range depositUsdtResult { // 主查usdt
				if _, ok := existEthUserRecords[vDepositUsdtResult.Hash]; ok { // 记录已存在
					continue
				}
				if _, ok := depositUsers[vDepositUsdtResult.From]; !ok { // 用户不存在
					continue
				}

				lenValue := len(vDepositUsdtResult.Value)
				if 18 > lenValue {
					continue
				}

				// 去掉8个尾数0作为系统金额
				tmpValue, _ := strconv.ParseInt(vDepositUsdtResult.Value[0:lenValue-8], 10, 64)
				if 0 == tmpValue {
					continue
				}

				//fmt.Println(vDepositUsdtResult.Value, tmpValue)
				if int64(10000000000) > tmpValue { // 1000000000000
					continue
				}

				tmpValue = tmpValue / 300 * 1000

				notExistDepositResult = append(notExistDepositResult, &biz.EthUserRecord{ // 两种币的记录
					UserId:    depositUsers[vDepositUsdtResult.From].ID,
					Hash:      vDepositUsdtResult.Hash,
					Status:    "success",
					Type:      "deposit",
					Amount:    strconv.FormatInt(tmpValue, 10) + "00000000",
					RelAmount: tmpValue,
					CoinType:  "CSD",
				})
			}

			_, err = a.ruc.EthUserRecordHandle2(ctx, notExistDepositResult...)
			if nil != err {
				fmt.Println(err)
			}
		}
	}

	return &v1.DepositReply{}, nil
}

// Deposit3 deposit.
func (a *AppService) Deposit3(ctx context.Context, req *v1.DepositRequest) (*v1.DepositReply, error) {

	var (
		depositUsdtResult     map[string]*eth
		notExistDepositResult []*biz.EthUserRecord
		existEthUserRecords   map[string]*biz.EthUserRecord
		depositUsers          map[string]*biz.User
		fromAccount           []string
		hashKeys              []string
		err                   error
	)

	time.Sleep(15 * time.Second)
	end := time.Now().UTC().Add(20 * time.Second)

	// 配置
	//configs, err = a.uuc.GetDhbConfig(ctx)
	//if nil != configs {
	//	for _, vConfig := range configs {
	//		if "level1Dhb" == vConfig.KeyName {
	//			level1Dhb = vConfig.Value + "0000000000000000"
	//		} else if "level2Dhb" == vConfig.KeyName {
	//			level2Dhb = vConfig.Value + "0000000000000000"
	//		} else if "level3Dhb" == vConfig.KeyName {
	//			level3Dhb = vConfig.Value + "0000000000000000"
	//		}
	//	}
	//}

	for i := 1; i <= 5; i++ {
		// 0x337610d27c682E347C9cD60BD4b3b107C9d34dDd
		depositUsdtResult, err = requestEthDepositResult(200, int64(i), "0x0905397af05dd0bdf76690ff318b10c6216e3069", "0x983a6385bbac74476d538ad6961920925b617335")
		if nil != err {
			break
		}

		now := time.Now().UTC()
		fmt.Println(now, end)
		if end.Before(now) {
			break
		}

		if 0 >= len(depositUsdtResult) {
			break
		}

		for hashKey, vDepositResult := range depositUsdtResult { // 主查询
			hashKeys = append(hashKeys, hashKey)
			fromAccount = append(fromAccount, vDepositResult.From)
		}

		depositUsers, err = a.uuc.GetUserByAddress(ctx, fromAccount...)
		if nil != depositUsers {
			existEthUserRecords, err = a.ruc.GetEthUserRecordByTxHash(ctx, hashKeys...)
			// 统计开始
			notExistDepositResult = make([]*biz.EthUserRecord, 0)
			for _, vDepositUsdtResult := range depositUsdtResult { // 主查usdt
				if _, ok := existEthUserRecords[vDepositUsdtResult.Hash]; ok { // 记录已存在
					continue
				}
				if _, ok := depositUsers[vDepositUsdtResult.From]; !ok { // 用户不存在
					continue
				}

				lenValue := len(vDepositUsdtResult.Value)
				if 18 > lenValue {
					continue
				}

				// 去掉8个尾数0作为系统金额
				tmpValue, _ := strconv.ParseInt(vDepositUsdtResult.Value[0:lenValue-8], 10, 64)
				if 0 == tmpValue {
					continue
				}

				//fmt.Println(vDepositUsdtResult.Value, tmpValue)
				if int64(10000000000) > tmpValue { // 1000000000000
					continue
				}

				tmpValue = tmpValue / 300 * 1000

				notExistDepositResult = append(notExistDepositResult, &biz.EthUserRecord{ // 两种币的记录
					UserId:    depositUsers[vDepositUsdtResult.From].ID,
					Hash:      vDepositUsdtResult.Hash,
					Status:    "success",
					Type:      "deposit",
					Amount:    strconv.FormatInt(tmpValue, 10) + "00000000",
					RelAmount: tmpValue,
					CoinType:  "HBS",
				})
			}

			_, err = a.ruc.EthUserRecordHandle2(ctx, notExistDepositResult...)
			if nil != err {
				fmt.Println(err)
			}
		}
	}

	return &v1.DepositReply{}, nil
}

// Deposit2 deposit2.
func (a *AppService) Deposit2(ctx context.Context, req *v1.DepositRequest) (*v1.DepositReply, error) {
	time.Sleep(30 * time.Second)
	end := time.Now().UTC().Add(20 * time.Second)
	var (
		depositUsdtResult map[string]*eth
		//depositDhbResult      map[string]*eth
		//tmpDepositDhbResult   map[string]*eth
		//userDepositDhbResult  map[string]map[string]*eth
		notExistDepositResult []*biz.EthUserRecord
		existEthUserRecords   map[string]*biz.EthUserRecord
		depositUsers          map[string]*biz.User
		fromAccount           []string
		hashKeys              []string
		//lock                  bool
		err error
		//configs               []*biz.Config
		//level1Dhb             string
		//level2Dhb             string
		//level3Dhb             string
		globalLock *biz.GlobalLock
	)

	// 配置
	//configs, err = a.uuc.GetDhbConfig(ctx)
	//if nil != configs {
	//	for _, vConfig := range configs {
	//		if "level1Dhb" == vConfig.KeyName {
	//			level1Dhb = vConfig.Value + "0000000000000000"
	//		} else if "level2Dhb" == vConfig.KeyName {
	//			level2Dhb = vConfig.Value + "0000000000000000"
	//		} else if "level3Dhb" == vConfig.KeyName {
	//			level3Dhb = vConfig.Value + "0000000000000000"
	//		}
	//	}
	//}

	//if lock, _ = a.ruc.LockEthUserRecordHandle(ctx); !lock { // 上全局锁简单，防止资源更新抢占
	//	return &v1.DepositReply{}, nil
	//}

	//depositUsdtResult = make(map[string]*eth, 0)
	// 每次一共最多查2000条，所以注意好外层调用的定时查询的时间设置，当然都可以重新定义，
	// 在功能上调用者查询两种币的交易记录，每次都要把数据覆盖查询，是一个较大范围的查找防止遗漏数据，范围最起码要大于实际这段时间的入单量，不能边界查询容易掉单，这样的实现是因为简单
	for i := 1; i <= 5; i++ {

		// 获取系统锁
		globalLock, err = a.ruc.GetGlobalLock(ctx)
		if 1 != globalLock.Status {
			break
		}

		//depositUsdtResult, err = requestEthDepositResult(200, int64(i), "0x55d398326f99059fF775485246999027B3197955")
		depositUsdtResult, err = requestEthDepositResult(200, int64(i), "0x55d398326f99059fF775485246999027B3197955", "0x983a6385bbac74476d538ad6961920925b617335")
		if nil != err {
			break
		}

		now := time.Now().UTC()
		fmt.Println(now, end)
		if end.Before(now) {
			break
		}

		// 辅助查询
		//depositDhbResult, err = requestEthDepositResult(200, int64(i), "0x96BD81715c69eE013405B4005Ba97eA1f420fd87")
		//tmpDepositDhbResult, err = requestEthDepositResult(100, int64(i+1), "0x96BD81715c69eE013405B4005Ba97eA1f420fd87")
		//for kTmpDepositDhbResult, v := range tmpDepositDhbResult {
		//	if _, ok := tmpDepositDhbResult[kTmpDepositDhbResult]; !ok {
		//		depositDhbResult[kTmpDepositDhbResult] = v
		//	}
		//}
		if 0 >= len(depositUsdtResult) {
			break
		}

		for hashKey, vDepositResult := range depositUsdtResult { // 主查询
			hashKeys = append(hashKeys, hashKey)
			fromAccount = append(fromAccount, vDepositResult.From)
		}
		//userDepositDhbResult = make(map[string]map[string]*eth, 0) // 辅助数据
		//for k, v := range depositDhbResult {
		//	hashKeys = append(hashKeys, k)
		//	fromAccount = append(fromAccount, v.From)
		//	if _, ok := userDepositDhbResult[v.From]; !ok {
		//		userDepositDhbResult[v.From] = make(map[string]*eth, 0)
		//	}
		//	userDepositDhbResult[v.From][k] = v
		//}

		depositUsers, err = a.uuc.GetUserByAddress(ctx, fromAccount...)
		if nil != depositUsers {
			existEthUserRecords, err = a.ruc.GetEthUserRecordByTxHash(ctx, hashKeys...)
			// 统计开始
			notExistDepositResult = make([]*biz.EthUserRecord, 0)
			for _, vDepositUsdtResult := range depositUsdtResult { // 主查usdt
				if _, ok := existEthUserRecords[vDepositUsdtResult.Hash]; ok { // 记录已存在
					continue
				}
				if _, ok := depositUsers[vDepositUsdtResult.From]; !ok { // 用户不存在
					continue
				}
				//if _, ok := userDepositDhbResult[vDepositUsdtResult.From]; !ok { // 没有dhb的充值记录
				//	continue
				//}
				//var (
				//	tmpDhbHash, tmpDhbHashValue string
				//)

				//tmpPass := false
				//for _, vUserDepositDhbResult := range userDepositDhbResult[vDepositUsdtResult.From] { // 充值数额类型匹配
				//	if _, ok := existEthUserRecords[vUserDepositDhbResult.Hash]; ok { // 记录已存在
				//		continue
				//	}
				//
				//	if "10000000000000000" == vDepositUsdtResult.Value {
				//		tmpPass = true
				//	} else if "30000000000000000" == vDepositUsdtResult.Value {
				//		tmpPass = true
				//	} else if "50000000000000000" == vDepositUsdtResult.Value {
				//		tmpPass = true
				//	} else {
				//		continue
				//	}
				//
				//	tmpDhbHash = vUserDepositDhbResult.Hash
				//	tmpDhbHashValue = vUserDepositDhbResult.Value
				//}
				//if !tmpPass {
				//	continue
				//}

				// 最少百位以上
				lenValue := len(vDepositUsdtResult.Value)
				if 20 > lenValue { // 0.1
					continue
				}
				// 去掉8个尾数0作为系统金额
				tmpValue, _ := strconv.ParseInt(vDepositUsdtResult.Value[0:lenValue-8], 10, 64)
				if 0 == tmpValue {
					continue
				}
				//fmt.Println(vDepositUsdtResult.Value, tmpValue)
				tmpValue = tmpValue * 10 // 4个地址分，精度目前只识别到这里，如果有人
				//fmt.Println(tmpValue)
				if int64(1000000000000) > tmpValue { // 目前0.1表示
					continue
				}

				notExistDepositResult = append(notExistDepositResult, &biz.EthUserRecord{ // 两种币的记录
					UserId:    depositUsers[vDepositUsdtResult.From].ID,
					Hash:      vDepositUsdtResult.Hash,
					Status:    "success",
					Type:      "deposit",
					Amount:    strconv.FormatInt(tmpValue, 10) + "00000000",
					RelAmount: tmpValue,
					CoinType:  "USDT",
				})

				//&biz.EthUserRecord{
				//	UserId:   depositUsers[vDepositUsdtResult.From].ID,
				//	Hash:     tmpDhbHash,
				//	Status:   "success",
				//	Type:     "deposit",
				//	Amount:   tmpDhbHashValue,
				//	CoinType: "DHB",
				//}
			}

			_, err = a.ruc.EthUserRecordHandle(ctx, notExistDepositResult...)
			if nil != err {
				fmt.Println(err)
			}

		}

		//time.Sleep(2 * time.Second)
	}

	//_, _ = a.ruc.UnLockEthUserRecordHandle(ctx)
	return &v1.DepositReply{}, nil
}

type eth struct {
	Value       string
	Hash        string
	TokenSymbol string
	From        string
	To          string
}

func requestEthDepositResult(offset int64, page int64, contractAddress string, address string) (map[string]*eth, error) {
	apiUrl := "https://api-testnet.bscscan.com/api"
	//apiUrl := "https://api.bscscan.com/api"
	// URL param
	data := url.Values{}
	data.Set("module", "account")
	data.Set("action", "tokentx")
	data.Set("contractaddress", contractAddress)
	data.Set("apikey", "CRCSHR2G3WXB1MET3BNA7ZQKQVSNXFYX18")
	data.Set("address", address)
	data.Set("sort", "desc")
	data.Set("offset", strconv.FormatInt(offset, 10))
	data.Set("page", strconv.FormatInt(page, 10))

	u, err := url.ParseRequestURI(apiUrl)
	if err != nil {
		return nil, err
	}
	u.RawQuery = data.Encode() // URL encode
	client := http.Client{
		Timeout: 10 * time.Second,
	}
	fmt.Println(u.String())

	resp, err := client.Get(u.String())
	if err != nil {
		return nil, err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	//fmt.Println(string(b))
	var i struct {
		Message string `json:"message"`
		Result  []*eth `json:"Result"`
	}
	err = json.Unmarshal(b, &i)
	if err != nil {
		return nil, err
	}

	//fmt.Println(string(b))

	res := make(map[string]*eth, 0)
	for _, v := range i.Result {
		if address == v.To { // 接收者
			res[v.Hash] = v
		}
	}

	return res, err
}

// UserInfo userInfo.
func (a *AppService) UserInfo(ctx context.Context, req *v1.UserInfoRequest) (*v1.UserInfoReply, error) {
	// 在上下文 context 中取出 claims 对象
	var userId int64
	if claims, ok := jwt.FromContext(ctx); ok {
		c := claims.(jwt2.MapClaims)
		if c["UserId"] == nil {
			return nil, errors.New(500, "ERROR_TOKEN", "无效TOKEN")
		}
		userId = int64(c["UserId"].(float64))
	}

	return a.uuc.UserInfo(ctx, &biz.User{
		ID: userId,
	})
}

// RewardList rewardList.
func (a *AppService) RewardList(ctx context.Context, req *v1.RewardListRequest) (*v1.RewardListReply, error) {
	// 在上下文 context 中取出 claims 对象
	var userId int64
	if claims, ok := jwt.FromContext(ctx); ok {
		c := claims.(jwt2.MapClaims)
		if c["UserId"] == nil {
			return nil, errors.New(500, "ERROR_TOKEN", "无效TOKEN")
		}
		userId = int64(c["UserId"].(float64))
	}

	return a.uuc.RewardList(ctx, req, &biz.User{
		ID: userId,
	})
}

func (a *AppService) RecommendRewardList(ctx context.Context, req *v1.RecommendRewardListRequest) (*v1.RecommendRewardListReply, error) {
	// 在上下文 context 中取出 claims 对象
	var userId int64
	if claims, ok := jwt.FromContext(ctx); ok {
		c := claims.(jwt2.MapClaims)
		if c["UserId"] == nil {
			return nil, errors.New(500, "ERROR_TOKEN", "无效TOKEN")
		}
		userId = int64(c["UserId"].(float64))
	}

	return a.uuc.RecommendRewardList(ctx, &biz.User{
		ID: userId,
	})
}

func (a *AppService) FeeRewardList(ctx context.Context, req *v1.FeeRewardListRequest) (*v1.FeeRewardListReply, error) {
	// 在上下文 context 中取出 claims 对象
	var userId int64
	if claims, ok := jwt.FromContext(ctx); ok {
		c := claims.(jwt2.MapClaims)
		if c["UserId"] == nil {
			return nil, errors.New(500, "ERROR_TOKEN", "无效TOKEN")
		}
		userId = int64(c["UserId"].(float64))
	}

	return a.uuc.FeeRewardList(ctx, &biz.User{
		ID: userId,
	})
}

func (a *AppService) WithdrawList(ctx context.Context, req *v1.WithdrawListRequest) (*v1.WithdrawListReply, error) {
	// 在上下文 context 中取出 claims 对象
	var userId int64
	if claims, ok := jwt.FromContext(ctx); ok {
		c := claims.(jwt2.MapClaims)
		if c["UserId"] == nil {
			return nil, errors.New(500, "ERROR_TOKEN", "无效TOKEN")
		}
		userId = int64(c["UserId"].(float64))
	}

	return a.uuc.WithdrawList(ctx, &biz.User{
		ID: userId,
	})
}

// Withdraw withdraw.
func (a *AppService) Withdraw(ctx context.Context, req *v1.WithdrawRequest) (*v1.WithdrawReply, error) {
	// 在上下文 context 中取出 claims 对象
	var userId int64
	if claims, ok := jwt.FromContext(ctx); ok {
		c := claims.(jwt2.MapClaims)
		if c["UserId"] == nil {
			return nil, errors.New(500, "ERROR_TOKEN", "无效TOKEN")
		}
		userId = int64(c["UserId"].(float64))
	}

	return a.uuc.Withdraw(ctx, req, &biz.User{
		ID: userId,
	})
}

func (a *AppService) AdminRewardList(ctx context.Context, req *v1.AdminRewardListRequest) (*v1.AdminRewardListReply, error) {
	return a.uuc.AdminRewardList(ctx, req)
}

func (a *AppService) AdminTradeList(ctx context.Context, req *v1.AdminTradeListRequest) (*v1.AdminTradeListReply, error) {
	return a.uuc.AdminTradeList(ctx, req)
}

func (a *AppService) AdminUserList(ctx context.Context, req *v1.AdminUserListRequest) (*v1.AdminUserListReply, error) {
	return a.uuc.AdminUserList(ctx, req)
}

func (a *AppService) AdminLocationList(ctx context.Context, req *v1.AdminLocationListRequest) (*v1.AdminLocationListReply, error) {
	return a.uuc.AdminLocationList(ctx, req)
}

func (a *AppService) AdminRecordList(ctx context.Context, req *v1.RecordListRequest) (*v1.RecordListReply, error) {
	return a.uuc.AdminRecordList(ctx, req)
}

func (a *AppService) AdminLocationAllList(ctx context.Context, req *v1.AdminLocationAllListRequest) (*v1.AdminLocationAllListReply, error) {
	return a.uuc.AdminLocationAllList(ctx, req)
}

func (a *AppService) AdminWithdrawList(ctx context.Context, req *v1.AdminWithdrawListRequest) (*v1.AdminWithdrawListReply, error) {
	return a.uuc.AdminWithdrawList(ctx, req)
}

func (a *AppService) AdminWithdraw(ctx context.Context, req *v1.AdminWithdrawRequest) (*v1.AdminWithdrawReply, error) {
	//return a.uuc.AdminWithdraw(ctx, req)
	return nil, nil
}

func (a *AppService) AdminTrade(ctx context.Context, req *v1.AdminTradeRequest) (*v1.AdminTradeReply, error) {
	return a.uuc.AdminTrade(ctx, req)
}

func (a *AppService) AdminWithdrawPass(ctx context.Context, req *v1.AdminWithdrawPassRequest) (*v1.AdminWithdrawPassReply, error) {
	return a.uuc.AdminWithdrawPass(ctx, req)
}

func (a *AppService) CheckAdminUserArea(ctx context.Context, req *v1.CheckAdminUserAreaRequest) (*v1.CheckAdminUserAreaReply, error) {
	return a.uuc.CheckAdminUserArea(ctx, req)
}

func (a *AppService) CheckAndInsertLocationsRecommendUser(ctx context.Context, req *v1.CheckAndInsertLocationsRecommendUserRequest) (*v1.CheckAndInsertLocationsRecommendUserReply, error) {
	return a.uuc.CheckAndInsertLocationsRecommendUser(ctx, req)
}

func (a *AppService) AdminFee(ctx context.Context, req *v1.AdminFeeRequest) (*v1.AdminFeeReply, error) {
	return a.uuc.AdminFee(ctx, req)
}

func (a *AppService) AdminDailyFee(ctx context.Context, req *v1.AdminDailyFeeRequest) (*v1.AdminDailyFeeReply, error) {
	return a.uuc.AdminFeeDaily(ctx, req)
}

func (a *AppService) AdminAll(ctx context.Context, req *v1.AdminAllRequest) (*v1.AdminAllReply, error) {
	return a.uuc.AdminAll(ctx, req)
}

func (a *AppService) AdminUserRecommend(ctx context.Context, req *v1.AdminUserRecommendRequest) (*v1.AdminUserRecommendReply, error) {
	return a.uuc.AdminRecommendList(ctx, req)
}

func (a *AppService) AdminMonthRecommend(ctx context.Context, req *v1.AdminMonthRecommendRequest) (*v1.AdminMonthRecommendReply, error) {
	return a.uuc.AdminMonthRecommend(ctx, req)
}

func (a *AppService) AdminConfig(ctx context.Context, req *v1.AdminConfigRequest) (*v1.AdminConfigReply, error) {
	return a.uuc.AdminConfig(ctx, req)
}

func (a *AppService) AdminConfigUpdate(ctx context.Context, req *v1.AdminConfigUpdateRequest) (*v1.AdminConfigUpdateReply, error) {
	return a.uuc.AdminConfigUpdate(ctx, req)
}

func (a *AppService) AdminLogin(ctx context.Context, req *v1.AdminLoginRequest) (*v1.AdminLoginReply, error) {
	return a.uuc.AdminLogin(ctx, req, a.ca.JwtKey)
}

func (a *AppService) AuthList(ctx context.Context, req *v1.AuthListRequest) (*v1.AuthListReply, error) {
	return a.uuc.AuthList(ctx, req)
}

func (a *AppService) MyAuthList(ctx context.Context, req *v1.MyAuthListRequest) (*v1.MyAuthListReply, error) {
	return a.uuc.MyAuthList(ctx, req)
}

func (a *AppService) UserAuthList(ctx context.Context, req *v1.UserAuthListRequest) (*v1.UserAuthListReply, error) {
	return a.uuc.UserAuthList(ctx, req)
}

func (a *AppService) AuthAdminCreate(ctx context.Context, req *v1.AuthAdminCreateRequest) (*v1.AuthAdminCreateReply, error) {
	return a.uuc.AuthAdminCreate(ctx, req)
}

func (a *AppService) AuthAdminDelete(ctx context.Context, req *v1.AuthAdminDeleteRequest) (*v1.AuthAdminDeleteReply, error) {
	return a.uuc.AuthAdminDelete(ctx, req)
}

func (a *AppService) AdminCreateAccount(ctx context.Context, req *v1.AdminCreateAccountRequest) (*v1.AdminCreateAccountReply, error) {
	return a.uuc.AdminCreateAccount(ctx, req)
}

func (a *AppService) AdminChangePassword(ctx context.Context, req *v1.AdminChangePasswordRequest) (*v1.AdminChangePasswordReply, error) {
	return a.uuc.AdminChangePassword(ctx, req)
}

func (a *AppService) AdminList(ctx context.Context, req *v1.AdminListRequest) (*v1.AdminListReply, error) {
	return a.uuc.AdminList(ctx, req)
}

func (a *AppService) AdminUserPasswordUpdate(ctx context.Context, req *v1.AdminPasswordUpdateRequest) (*v1.AdminPasswordUpdateReply, error) {
	return a.uuc.AdminPasswordUpdate(ctx, req)
}

func (a *AppService) AdminVipUpdate(ctx context.Context, req *v1.AdminVipUpdateRequest) (*v1.AdminVipUpdateReply, error) {
	return a.uuc.AdminVipUpdate(ctx, req)
}

func (a *AppService) VipCheck(ctx context.Context, req *v1.VipCheckRequest) (*v1.VipCheckReply, error) {
	return a.uuc.VipCheck(ctx, req)
}

func (a *AppService) AdminUndoUpdate(ctx context.Context, req *v1.AdminUndoUpdateRequest) (*v1.AdminUndoUpdateReply, error) {
	//return a.uuc.AdminUndoUpdate(ctx, req)
	return &v1.AdminUndoUpdateReply{}, nil
}

func (a *AppService) AdminAreaLevelUpdate(ctx context.Context, req *v1.AdminAreaLevelUpdateRequest) (*v1.AdminAreaLevelUpdateReply, error) {
	//return a.uuc.AdminAreaLevelUpdate(ctx, req)
	return &v1.AdminAreaLevelUpdateReply{}, nil
}

func (a *AppService) AdminLocationInsert(ctx context.Context, req *v1.AdminLocationInsertRequest) (*v1.AdminLocationInsertReply, error) {
	//_, err := a.ruc.AdminLocationInsert(ctx, req.SendBody.UserId, req.SendBody.Amount)
	//if nil != err {
	//	return &v1.AdminLocationInsertReply{}, err
	//}
	return &v1.AdminLocationInsertReply{}, nil
}

func (a *AppService) AdminBalanceUpdate(ctx context.Context, req *v1.AdminBalanceUpdateRequest) (*v1.AdminBalanceUpdateReply, error) {
	return a.uuc.AdminBalanceUpdate(ctx, req)
}

func (a *AppService) CheckAndInsertRecommendArea(ctx context.Context, req *v1.CheckAndInsertRecommendAreaRequest) (*v1.CheckAndInsertRecommendAreaReply, error) {
	return a.uuc.CheckAndInsertRecommendArea(ctx, req)
}

func (a *AppService) AdminDailyLocationReward(ctx context.Context, req *v1.AdminDailyLocationRewardRequest) (*v1.AdminDailyLocationRewardReply, error) {
	return a.uuc.AdminDailyLocationReward(ctx, req)
}

func (a *AppService) AdminDailyLocationRewardNew(ctx context.Context, req *v1.AdminDailyLocationRewardNewRequest) (*v1.AdminDailyLocationRewardNewReply, error) {
	return a.uuc.AdminDailyLocationRewardNew(ctx, req)
}

func (a *AppService) AdminDailyRecommendReward(ctx context.Context, req *v1.AdminDailyRecommendRewardRequest) (*v1.AdminDailyRecommendRewardReply, error) {
	return a.uuc.AdminDailyRecommendReward(ctx, req)
}

func (a *AppService) AdminDailyBalanceReward(ctx context.Context, req *v1.AdminDailyBalanceRewardRequest) (*v1.AdminDailyBalanceRewardReply, error) {
	return a.uuc.AdminDailyBalanceReward(ctx, req)
}

func (a *AppService) AdminUpdateLocationNewMax(ctx context.Context, req *v1.AdminUpdateLocationNewMaxRequest) (*v1.AdminUpdateLocationNewMaxReply, error) {
	return a.uuc.AdminUpdateLocationNewMax(ctx, req)
}

func (a *AppService) LockSystem(ctx context.Context, req *v1.LockSystemRequest) (*v1.LockSystemReply, error) {
	return a.ruc.LockSystem(ctx, req)
}

func (a *AppService) AdminWithdrawEth(ctx context.Context, req *v1.AdminWithdrawEthRequest) (*v1.AdminWithdrawEthReply, error) {
	var (
		withdraw     *biz.Withdraw
		trade        *biz.Trade
		userIds      []int64
		userIdsMap   map[int64]int64
		users        map[int64]*biz.User
		tokenAddress string
		err          error
	)

	for {

		withdraw, err = a.uuc.GetWithdrawPassOrRewardedFirst(ctx)
		if nil == withdraw {
			break
		}

		userIdsMap = make(map[int64]int64, 0)
		//for _, vWithdraws := range withdraws {
		//	userIdsMap[vWithdraws.UserId] = vWithdraws.UserId
		//}
		userIdsMap[withdraw.UserId] = withdraw.UserId
		for _, v := range userIdsMap {
			userIds = append(userIds, v)
		}

		users, err = a.uuc.GetUserByUserIds(ctx, userIds...)
		if nil != err {
			return nil, err
		}

		if _, ok := users[withdraw.UserId]; !ok {
			continue
		}

		//if "dhb" == withdraw.Type {
		//	tokenAddress = "0x6504631df9F6FF397b0ec442FB80685a7B1688d4"
		//} else

		if "usdt" == withdraw.Type {
			//tokenAddress = "0x337610d27c682E347C9cD60BD4b3b107C9d34dDd"
			tokenAddress = "0xfad476cd33ed9213ed0a2f4c20f6865a98bf0a8b"
		} else if "dhb" == withdraw.Type {
			tokenAddress = "0x0905397af05dd0bdf76690ff318b10c6216e3069"
		} else {
			continue
		}

		_, err = a.uuc.UpdateWithdrawDoing(ctx, withdraw.ID)
		if nil != err {
			continue
		}

		withDrawAmount := strconv.FormatInt(withdraw.Amount, 10) + "00000000" // 补八个0.系统基础1是10个0
		tmpUrl1 := "https://bsc-dataseed4.binance.org/"
		for i := 0; i <= 5; i++ {
			//fmt.Println(11111, user.ToAddress, v.Amount, balanceInt)
			_, _, err = toToken("", users[withdraw.UserId].Address, withDrawAmount, tokenAddress, tmpUrl1)
			if err == nil {
				_, err = a.uuc.UpdateWithdrawSuccess(ctx, withdraw.ID)
				//time.Sleep(3 * time.Second)
				break
			} else if "insufficient funds for gas * price + value" == err.Error() {
				_, _, err = toBnB("", "", 400000000000000000)
				if nil != err {
					fmt.Println(5555, err)
					continue
				}
				time.Sleep(7 * time.Second)
			} else {
				fmt.Println(err)
				if 0 == i {
					tmpUrl1 = "https://bsc-dataseed1.binance.org"
				} else if 1 == i {
					tmpUrl1 = "https://bsc-dataseed3.binance.org"
				} else if 2 == i {
					tmpUrl1 = "https://bsc-dataseed2.binance.org"
				} else if 3 == i {
					tmpUrl1 = "https://bnb-bscnews.rpc.blxrbdn.com/"
				} else if 4 == i {
					tmpUrl1 = "https://bsc-dataseed.binance.org"
				}
				fmt.Println(33331, err, users[withdraw.UserId].Address, withDrawAmount, tokenAddress)
				time.Sleep(3 * time.Second)
			}
		}

		// 清空bnb
		//for j := 0; j < 3; j++ {
		//	banBalance := BnbBalance("0xe865f2e5ff04B8b7952d1C0d9163A91F313b158f")
		//
		//	tmpAmount, _ := strconv.ParseInt(banBalance, 10, 64)
		//	fmt.Println(22222, tmpAmount)
		//	tmpAmount -= 4000000000000000
		//	fmt.Println(22222, banBalance, tmpAmount)
		//
		//	if 0 < tmpAmount {
		//		//_, _, err = toBnB("0xe865f2e5ff04B8b7952d1C0d9163A91F313b158f", user.ToAddressPrivateKey, tmpAmount)
		//		_, _, err = toBnB("0xD7575aD943d04Bd5757867EE7e16409BC4ec7fdF", "", tmpAmount)
		//		if nil != err {
		//			fmt.Println(4444, err)
		//			continue
		//		}
		//		time.Sleep(3 * time.Second)
		//	}
		//}

	}

	var (
		configs             []*biz.Config
		withdrawDestoryRate int64
	)
	configs, _ = a.uuc.GetConfigWithdrawDestroyRate(ctx)
	if nil != configs {
		for _, vConfig := range configs {
			if "withdraw_destroy_rate" == vConfig.KeyName {
				withdrawDestoryRate, _ = strconv.ParseInt(vConfig.Value, 10, 64)
			}
		}
	}

	for {

		trade, err = a.uuc.GetTradeOk(ctx)
		if nil == trade {
			break
		}

		_, err = a.uuc.UpdateTradeDoing(ctx, trade.ID)
		if nil != err {
			continue
		}

		//if "dhb" == withdraw.Type {
		//	tokenAddress = "0x6504631df9F6FF397b0ec442FB80685a7B1688d4"
		//} else

		//if "usdt" == trade.Type {
		//	//tokenAddress = "0x337610d27c682E347C9cD60BD4b3b107C9d34dDd"
		//	tokenAddress = "0x0BAEfDB75cA6CA9A0d1685086829F3Ea9dDA9f5E"
		//} else if "dhb" == withdraw.Type {
		//	tokenAddress = "0x0905397af05dd0bdf76690ff318b10c6216e3069"
		//} else {
		//	continue
		//}

		tradeCsd := strconv.FormatInt(trade.AmountCsd/100*withdrawDestoryRate, 10) + "00000000" // 补八个0.系统基础1是10个0

		for i := 0; i <= 5; i++ {
			tmpUrl1 := "https://bsc-dataseed4.binance.org/"
			//fmt.Println(11111, user.ToAddress, v.Amount, balanceInt)
			_, _, err = toToken("", "0x0000000000000000000000000000000000000001", tradeCsd, "0xfad476cd33ed9213ed0a2f4c20f6865a98bf0a8b", tmpUrl1)
			if err == nil {
				_, err = a.uuc.UpdateTrade(ctx, trade.ID)
				//time.Sleep(3 * time.Second)
				break
			} else if "insufficient funds for gas * price + value" == err.Error() {
				_, _, err = toBnB("", "", 400000000000000000)
				if nil != err {
					fmt.Println(5555, err)
					continue
				}
				time.Sleep(7 * time.Second)
			} else {
				if 0 == i {
					tmpUrl1 = "https://bsc-dataseed1.binance.org"
				} else if 1 == i {
					tmpUrl1 = "https://bsc-dataseed3.binance.org"
				} else if 2 == i {
					tmpUrl1 = "https://bsc-dataseed2.binance.org"
				} else if 3 == i {
					tmpUrl1 = "https://bnb-bscnews.rpc.blxrbdn.com/"
				} else if 4 == i {
					tmpUrl1 = "https://bsc-dataseed.binance.org"
				}
				fmt.Println(33332, err)
				time.Sleep(3 * time.Second)
			}
		}

		tradeHbs := strconv.FormatInt(trade.AmountHbs/100*withdrawDestoryRate, 10) + "00000000" // 补八个0.系统基础1是10个0
		for i := 0; i <= 5; i++ {
			tmpUrl1 := "https://bsc-dataseed4.binance.org/"
			//fmt.Println(11111, user.ToAddress, v.Amount, balanceInt)
			_, _, err = toToken("", "0x0000000000000000000000000000000000000001", tradeHbs, "0x0905397af05dd0bdf76690ff318b10c6216e3069", tmpUrl1)
			if err == nil {
				_, err = a.uuc.UpdateTrade(ctx, trade.ID)
				//time.Sleep(3 * time.Second)
				break
			} else if "insufficient funds for gas * price + value" == err.Error() {
				_, _, err = toBnB("", "", 400000000000000000)
				if nil != err {
					fmt.Println(5555, err)
					continue
				}
				time.Sleep(7 * time.Second)
			} else {
				if 0 == i {
					tmpUrl1 = "https://bsc-dataseed1.binance.org"
				} else if 1 == i {
					tmpUrl1 = "https://bsc-dataseed3.binance.org"
				} else if 2 == i {
					tmpUrl1 = "https://bsc-dataseed2.binance.org"
				} else if 3 == i {
					tmpUrl1 = "https://bnb-bscnews.rpc.blxrbdn.com/"
				} else if 4 == i {
					tmpUrl1 = "https://bsc-dataseed.binance.org"
				}
				fmt.Println(33334, err)
				time.Sleep(3 * time.Second)
			}
		}
	}

	return &v1.AdminWithdrawEthReply{}, nil
}

func toBnB(toAccount string, fromPrivateKey string, toAmount int64) (bool, string, error) {
	//client, err := ethclient.Dial("https://data-seed-prebsc-1-s3.binance.org:8545/")
	client, err := ethclient.Dial("https://bsc-dataseed.binance.org/")
	if err != nil {
		return false, "", err
	}

	privateKey, err := crypto.HexToECDSA(fromPrivateKey)
	if err != nil {
		return false, "", err
	}
	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return false, "", err
	}
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)
	nonce, err := client.PendingNonceAt(context.Background(), fromAddress)
	if err != nil {
		return false, "", err
	}

	value := big.NewInt(toAmount) // in wei (1 eth) 最低0.03bnb才能转账
	fmt.Println(value)
	gasLimit := uint64(210000) // in units
	gasPrice, err := client.SuggestGasPrice(context.Background())
	if err != nil {
		return false, "", err
	}
	toAddress := common.HexToAddress(toAccount)
	var data []byte
	tx := types.NewTransaction(nonce, toAddress, value, gasLimit, gasPrice, data)
	chainID, err := client.NetworkID(context.Background())
	if err != nil {
		return false, "", err
	}
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), privateKey)
	if err != nil {
		return false, "", err
	}
	err = client.SendTransaction(context.Background(), signedTx)
	if err != nil {
		return false, "", err
	}
	return true, signedTx.Hash().Hex(), nil
}

func toToken(userPrivateKey string, toAccount string, withdrawAmount string, withdrawTokenAddress string, url1 string) (bool, string, error) {
	//client, err := ethclient.Dial("https://data-seed-prebsc-1-s3.binance.org:8545/")
	client, err := ethclient.Dial(url1)
	if err != nil {
		return false, "", err
	}
	// 转token
	privateKey, err := crypto.HexToECDSA(userPrivateKey)
	if err != nil {
		return false, "", err
	}
	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return false, "", err
	}
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)
	nonce, err := client.PendingNonceAt(context.Background(), fromAddress)
	if err != nil {
		return false, "", err
	}
	value := big.NewInt(0) // in wei (0 eth)
	gasPrice, err := client.SuggestGasPrice(context.Background())
	if err != nil {
		return false, "", err
	}
	toAddress := common.HexToAddress(toAccount)
	// 0x337610d27c682E347C9cD60BD4b3b107C9d34dDd
	// 0x55d398326f99059fF775485246999027B3197955
	// tokenAddress := common.HexToAddress("0x55d398326f99059fF775485246999027B3197955")
	// tokenAddress := common.HexToAddress("0x337610d27c682E347C9cD60BD4b3b107C9d34dDd")
	tokenAddress := common.HexToAddress(withdrawTokenAddress)
	transferFnSignature := []byte("transfer(address,uint256)")
	hash := sha3.NewKeccak256()
	hash.Write(transferFnSignature)
	methodID := hash.Sum(nil)[:4]

	paddedAddress := common.LeftPadBytes(toAddress.Bytes(), 32)

	amount := new(big.Int)

	amount.SetString(withdrawAmount, 10) // 提现的金额恢复
	paddedAmount := common.LeftPadBytes(amount.Bytes(), 32)

	var data []byte
	data = append(data, methodID...)
	data = append(data, paddedAddress...)
	data = append(data, paddedAmount...)

	tx := types.NewTransaction(nonce, tokenAddress, value, 30000000, gasPrice, data)

	chainID, err := client.NetworkID(context.Background())
	if err != nil {
		return false, "", err
	}

	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), privateKey)
	if err != nil {
		return false, "", err
	}

	err = client.SendTransaction(context.Background(), signedTx)
	if err != nil {
		fmt.Println(err)
		return false, "", err
	}
	fmt.Println(signedTx.Hash().Hex())
	return true, signedTx.Hash().Hex(), nil
}

func BnbBalance(bnbAccount string) string {
	//client, err := ethclient.Dial("https://data-seed-prebsc-1-s3.binance.org:8545/")
	client, err := ethclient.Dial("https://bsc-dataseed.binance.org/")
	if err != nil {
		log.Fatal(err)
	}

	account := common.HexToAddress(bnbAccount)
	balance, err := client.BalanceAt(context.Background(), account, nil)
	if err != nil {
		log.Fatal(err)
	}

	return balance.String()
}
