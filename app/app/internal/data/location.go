package data

import (
	"context"
	"dhb/app/app/internal/biz"
	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
	"time"
)

type Location struct {
	ID            int64     `gorm:"primarykey;type:int"`
	UserId        int64     `gorm:"type:int;not null"`
	Row           int64     `gorm:"type:int;not null"`
	Col           int64     `gorm:"type:int;not null"`
	Status        string    `gorm:"type:varchar(45);not null"`
	CurrentLevel  int64     `gorm:"type:int;not null"`
	Current       int64     `gorm:"type:bigint;not null"`
	CurrentMax    int64     `gorm:"type:bigint;not null"`
	CurrentMaxNew int64     `gorm:"type:bigint;not null"`
	StopDate      time.Time `gorm:"type:datetime;not null"`
	CreatedAt     time.Time `gorm:"type:datetime;not null"`
	UpdatedAt     time.Time `gorm:"type:datetime;not null"`
}
type LocationNew struct {
	ID                int64     `gorm:"primarykey;type:int"`
	UserId            int64     `gorm:"type:int;not null"`
	Term              int64     `gorm:"type:int;not null"`
	Status            string    `gorm:"type:varchar(45);not null"`
	Current           int64     `gorm:"type:bigint;not null"`
	CurrentMax        int64     `gorm:"type:bigint;not null"`
	Usdt              int64     `gorm:"type:bigint;not null"`
	CurrentMaxNew     int64     `gorm:"type:bigint;not null"`
	StopLocationAgain int64     `gorm:"type:int;not null"`
	OutRate           int64     `gorm:"type:int;not null"`
	StopCoin          int64     `gorm:"type:bigint;not null"`
	StopDate          time.Time `gorm:"type:datetime;not null"`
	CreatedAt         time.Time `gorm:"type:datetime;not null"`
	UpdatedAt         time.Time `gorm:"type:datetime;not null"`
}

type GlobalLock struct {
	ID     int64 `gorm:"primarykey;type:int"`
	Status int64 `gorm:"type:int;not null"`
}

type LocationRepo struct {
	data *Data
	log  *log.Helper
}

func NewLocationRepo(data *Data, logger log.Logger) biz.LocationRepo {
	return &LocationRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
}

// CreateLocation .
func (lr *LocationRepo) CreateLocation(ctx context.Context, rel *biz.Location) (*biz.Location, error) {
	var location Location
	location.Col = rel.Col
	location.Row = rel.Row
	location.Status = rel.Status
	location.Current = rel.Current
	location.CurrentMax = rel.CurrentMax
	location.CurrentLevel = rel.CurrentLevel
	location.UserId = rel.UserId
	res := lr.data.DB(ctx).Table("location").Create(&location)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_LOCATION_ERROR", "占位信息创建失败")
	}

	return &biz.Location{
		ID:           location.ID,
		UserId:       location.UserId,
		Status:       location.Status,
		CurrentLevel: location.CurrentLevel,
		Current:      location.Current,
		CurrentMax:   location.CurrentMax,
		Row:          location.Row,
		Col:          location.Col,
	}, nil
}

// CreateLocationNew .
func (lr *LocationRepo) CreateLocationNew(ctx context.Context, rel *biz.LocationNew, amount int64) (*biz.LocationNew, error) {
	var location LocationNew
	location.Status = rel.Status
	location.Term = rel.Term
	location.Current = rel.Current
	location.CurrentMax = rel.CurrentMax
	location.UserId = rel.UserId
	location.OutRate = rel.OutRate
	location.StopDate = rel.StopDate
	location.Usdt = amount
	res := lr.data.DB(ctx).Table("location_new").Create(&location)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_LOCATION_ERROR", "占位信息创建失败")
	}

	var err error
	//if len(tmpRecommendUserIdsInt) > 0 {
	//	if err = lr.data.DB(ctx).Table("user_info").
	//		Where("user_id in (?)", tmpRecommendUserIdsInt).
	//		Updates(map[string]interface{}{"team_csd_balance": gorm.Expr("team_csd_balance + ?", rel.CurrentMax)}).Error; nil != err {
	//		return nil, errors.NotFound("user balance err", "user balance not found")
	//	}
	//}

	if err = lr.data.DB(ctx).Table("user_balance").
		Where("user_id=?", rel.UserId).
		Updates(map[string]interface{}{"balance_usdt": gorm.Expr("balance_usdt + ?", rel.CurrentMax)}).Error; nil != err {
		return nil, errors.NotFound("user balance err", "user balance not found")
	}

	var userBalanceRecode UserBalanceRecord
	userBalanceRecode.Balance = 0
	userBalanceRecode.UserId = rel.UserId
	userBalanceRecode.Type = "deposit"
	userBalanceRecode.CoinType = "usdt"
	userBalanceRecode.Amount = amount
	res = lr.data.DB(ctx).Table("user_balance_record").Create(&userBalanceRecode)
	if res.Error != nil {
		return nil, errors.New(500, "CREATE_LOCATION_ERROR", "占位信息创建失败")
	}

	return &biz.LocationNew{
		ID:         location.ID,
		UserId:     location.UserId,
		Status:     location.Status,
		Current:    location.Current,
		CurrentMax: location.CurrentMax,
		Term:       location.Term,
	}, nil
}

// GetLocationDailyYesterday .
func (lr *LocationRepo) GetLocationDailyYesterday(ctx context.Context, day int) ([]*biz.LocationNew, error) {
	var locations []*LocationNew
	res := make([]*biz.LocationNew, 0)
	instance := lr.data.db.Table("location_new")

	// 16点之后执行
	now := time.Now().UTC().AddDate(0, 0, day)
	startDate := now
	endDate := now.AddDate(0, 0, 1)
	todayStart := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 16, 0, 0, 0, time.UTC)
	todayEnd := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 16, 0, 0, 0, time.UTC)

	instance = instance.Where("created_at>=?", todayStart)
	instance = instance.Where("created_at<?", todayEnd)
	if err := instance.Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return res, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, v := range locations {
		res = append(res, &biz.LocationNew{
			ID:         v.ID,
			UserId:     v.UserId,
			Status:     v.Status,
			Current:    v.Current,
			CurrentMax: v.CurrentMax,
			OutRate:    v.OutRate,
		})
	}

	return res, nil
}

// GetLocationLast .
func (lr *LocationRepo) GetLocationLast(ctx context.Context) (*biz.Location, error) {
	var location Location
	if err := lr.data.db.Table("location").Where("status=?", "running").Order("id desc").First(&location).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	return &biz.Location{
		ID:           location.ID,
		UserId:       location.UserId,
		Status:       location.Status,
		CurrentLevel: location.CurrentLevel,
		Current:      location.Current,
		CurrentMax:   location.CurrentMax,
		Row:          location.Row,
		Col:          location.Col,
	}, nil
}

// GetMyLocationLast .
func (lr *LocationRepo) GetMyLocationLast(ctx context.Context, userId int64) (*biz.LocationNew, error) {
	var location LocationNew
	if err := lr.data.db.Table("location_new").Where("user_id", userId).Order("id desc").First(&location).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	return &biz.LocationNew{
		ID:         location.ID,
		UserId:     location.UserId,
		Status:     location.Status,
		Current:    location.Current,
		CurrentMax: location.CurrentMax,
		StopDate:   location.StopDate,
	}, nil
}

// GetMyStopLocationLast .
func (lr *LocationRepo) GetMyStopLocationLast(ctx context.Context, userId int64) (*biz.Location, error) {
	var location Location
	if err := lr.data.db.Table("location_new").
		Where("status=?", "stop").
		Where("user_id", userId).Order("id desc").First(&location).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	return &biz.Location{
		ID:           location.ID,
		UserId:       location.UserId,
		Status:       location.Status,
		CurrentLevel: location.CurrentLevel,
		Current:      location.Current,
		CurrentMax:   location.CurrentMax,
		Row:          location.Row,
		Col:          location.Col,
		StopDate:     location.StopDate,
	}, nil
}

// GetMyStopLocationsLast .
func (lr *LocationRepo) GetMyStopLocationsLast(ctx context.Context, userId int64) ([]*biz.LocationNew, error) {

	var locations []*LocationNew
	res := make([]*biz.LocationNew, 0)
	if err := lr.data.db.Table("location_new").
		Where("user_id", userId).
		Where("status=?", "stop").
		Where("stop_location_again", 0).
		Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, location := range locations {
		res = append(res, &biz.LocationNew{
			ID:                location.ID,
			UserId:            location.UserId,
			Status:            location.Status,
			Current:           location.Current,
			CurrentMax:        location.CurrentMax,
			StopDate:          location.StopDate,
			StopLocationAgain: location.StopLocationAgain,
			StopCoin:          location.StopCoin,
		})
	}

	return res, nil
}

// GetMyLocationRunningLast .
func (lr *LocationRepo) GetMyLocationRunningLast(ctx context.Context, userId int64) (*biz.Location, error) {
	var location Location
	if err := lr.data.db.Table("location").Where("user_id", userId).
		Where("status=?", "running").
		Order("id desc").First(&location).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	return &biz.Location{
		ID:           location.ID,
		UserId:       location.UserId,
		Status:       location.Status,
		CurrentLevel: location.CurrentLevel,
		Current:      location.Current,
		CurrentMax:   location.CurrentMax,
		Row:          location.Row,
		Col:          location.Col,
	}, nil
}

// GetLocationsByUserIds .
func (lr *LocationRepo) GetLocationsByUserIds(ctx context.Context, userIds []int64) ([]*biz.Location, error) {
	var locations []*Location
	res := make([]*biz.Location, 0)
	if err := lr.data.db.Table("location").
		Where("user_id IN(?)", userIds).
		Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, location := range locations {
		res = append(res, &biz.Location{
			ID:           location.ID,
			UserId:       location.UserId,
			Status:       location.Status,
			CurrentLevel: location.CurrentLevel,
			Current:      location.Current,
			CurrentMax:   location.CurrentMax,
			Row:          location.Row,
			Col:          location.Col,
		})
	}

	return res, nil
}

// GetAllLocations .
func (lr *LocationRepo) GetAllLocations(ctx context.Context) ([]*biz.Location, error) {
	var locations []*Location
	res := make([]*biz.Location, 0)
	if err := lr.data.db.Table("location").
		Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, location := range locations {
		res = append(res, &biz.Location{
			ID:           location.ID,
			UserId:       location.UserId,
			Status:       location.Status,
			CurrentLevel: location.CurrentLevel,
			Current:      location.Current,
			CurrentMax:   location.CurrentMax,
			Row:          location.Row,
			Col:          location.Col,
		})
	}

	return res, nil
}

// GetAllLocationsNew .
func (lr *LocationRepo) GetAllLocationsNew(ctx context.Context) ([]*biz.LocationNew, error) {
	var locations []*LocationNew
	res := make([]*biz.LocationNew, 0)
	if err := lr.data.db.Table("location_new").
		Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, location := range locations {
		res = append(res, &biz.LocationNew{
			ID:            location.ID,
			UserId:        location.UserId,
			Term:          location.Term,
			Current:       location.Current,
			CurrentMax:    location.CurrentMax,
			CurrentMaxNew: location.CurrentMaxNew,
		})
	}

	return res, nil
}

// GetLocationsByUserId .
func (lr *LocationRepo) GetLocationsByUserId(ctx context.Context, userId int64) ([]*biz.Location, error) {
	var locations []*Location
	res := make([]*biz.Location, 0)
	if err := lr.data.db.Table("location_new").
		Where("user_id=?", userId).
		Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, location := range locations {
		res = append(res, &biz.Location{
			ID:            location.ID,
			UserId:        location.UserId,
			Status:        location.Status,
			CurrentLevel:  location.CurrentLevel,
			Current:       location.Current,
			CurrentMax:    location.CurrentMax,
			CurrentMaxNew: location.CurrentMaxNew,
			Row:           location.Row,
			Col:           location.Col,
		})
	}

	return res, nil
}

// GetLocationsNewByUserId .
func (lr *LocationRepo) GetLocationsNewByUserId(ctx context.Context, userId int64) ([]*biz.LocationNew, error) {
	var locations []*LocationNew
	res := make([]*biz.LocationNew, 0)
	if err := lr.data.db.Table("location_new").
		Where("user_id=?", userId).
		Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, location := range locations {
		res = append(res, &biz.LocationNew{
			ID:         location.ID,
			UserId:     location.UserId,
			Status:     location.Status,
			Current:    location.Current,
			CurrentMax: location.CurrentMax,
			OutRate:    location.OutRate,
			Term:       location.Term,
		})
	}

	return res, nil
}

// GetLocationsStopNotUpdate .
func (lr *LocationRepo) GetLocationsStopNotUpdate(ctx context.Context) ([]*biz.Location, error) {
	var locations []*Location
	res := make([]*biz.Location, 0)
	if err := lr.data.db.Table("location").
		Where("status=?", "stop").
		Where("stop_is_update=?", 0).
		Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, location := range locations {
		res = append(res, &biz.Location{
			ID:           location.ID,
			UserId:       location.UserId,
			Status:       location.Status,
			CurrentLevel: location.CurrentLevel,
			Current:      location.Current,
			CurrentMax:   location.CurrentMax,
			Row:          location.Row,
			Col:          location.Col,
		})
	}

	return res, nil
}

// LockGlobalLocation .
func (lr *LocationRepo) LockGlobalLocation(ctx context.Context) (bool, error) {
	res := lr.data.DB(ctx).Where("id=?", 1).
		Table("global_lock").
		Updates(map[string]interface{}{"status": 2})

	if 0 <= res.RowsAffected {
		return true, nil
	}

	return false, res.Error

}

// UnLockGlobalLocation .
func (lr *LocationRepo) UnLockGlobalLocation(ctx context.Context) (bool, error) {
	res := lr.data.DB(ctx).Where("id=? and status=?", 1, 1).
		Table("global_lock").
		Updates(map[string]interface{}{"status": 2})

	if 0 <= res.RowsAffected {
		return true, nil
	}

	return false, res.Error
}

// LockGlobalWithdraw .
func (lr *LocationRepo) LockGlobalWithdraw(ctx context.Context) (bool, error) {
	res := lr.data.DB(ctx).Where("id=? and status>=?", 1, 2).
		Table("global_lock").
		Updates(map[string]interface{}{"status": 3})

	if 0 <= res.RowsAffected {
		return true, nil
	}

	return false, res.Error
}

// GetLockGlobalLocation .
func (lr *LocationRepo) GetLockGlobalLocation(ctx context.Context) (*biz.GlobalLock, error) {
	var globalLock GlobalLock
	if res := lr.data.DB(ctx).Where("id=?", 1).
		Table("global_lock").
		First(&globalLock); res.Error != nil {
		return nil, res.Error
	}

	return &biz.GlobalLock{
		ID:     globalLock.ID,
		Status: globalLock.Status,
	}, nil
}

// UnLockGlobalWithdraw .
func (lr *LocationRepo) UnLockGlobalWithdraw(ctx context.Context) (bool, error) {
	res := lr.data.DB(ctx).Where("id=? and status=?", 1, 3).
		Table("global_lock").
		Updates(map[string]interface{}{"status": 2})

	if 0 <= res.RowsAffected {
		return true, nil
	}

	return false, res.Error
}

// UpdateLocation .
func (lr *LocationRepo) UpdateLocation(ctx context.Context, id int64, status string, current int64, stopDate time.Time) error {

	if "stop" == status {
		res := lr.data.db.Table("location").
			Where("id=?", id).
			Updates(map[string]interface{}{"current": gorm.Expr("current + ?", current), "status": "stop", "stop_date": stopDate})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	} else {
		res := lr.data.db.Table("location").
			Where("id=?", id).
			Where("status=?", "running").
			Updates(map[string]interface{}{"current": gorm.Expr("current + ?", current), "status": status})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	}

	return nil
}

// UpdateLocationNew .
func (lr *LocationRepo) UpdateLocationNew(ctx context.Context, id int64, status string, current int64, stopDate time.Time) error {

	if "stop" == status {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Updates(map[string]interface{}{"current": gorm.Expr("current + ?", current), "status": "stop", "stop_date": stopDate})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	} else {
		res := lr.data.DB(ctx).Table("location_new").
			Where("id=?", id).
			Where("status=?", "running").
			Updates(map[string]interface{}{"current": gorm.Expr("current + ?", current), "status": status})
		if 0 == res.RowsAffected || res.Error != nil {
			return res.Error
		}
	}

	return nil
}

// UpdateLocationNewCurrent .
func (lr *LocationRepo) UpdateLocationNewCurrent(ctx context.Context, id int64, current int64) error {

	res := lr.data.DB(ctx).Table("location_new").
		Where("id=?", id).
		Updates(map[string]interface{}{"current": gorm.Expr("current + ?", current)})
	if 0 == res.RowsAffected || res.Error != nil {
		return res.Error
	}

	return nil
}

// GetRunningLocations .
func (lr *LocationRepo) GetRunningLocations(ctx context.Context) ([]*biz.LocationNew, error) {
	var locations []*LocationNew
	res := make([]*biz.LocationNew, 0)
	if err := lr.data.db.Table("location_new").
		Where("status=?", "running").
		Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return res, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	for _, location := range locations {
		res = append(res, &biz.LocationNew{
			ID:         location.ID,
			UserId:     location.UserId,
			Status:     location.Status,
			Current:    location.Current,
			CurrentMax: location.CurrentMax,
			CreatedAt:  location.CreatedAt,
			OutRate:    location.OutRate,
		})
	}

	return res, nil
}

// UpdateLocationRowAndCol 事务中使用 .
func (lr *LocationRepo) UpdateLocationRowAndCol(ctx context.Context, id int64) error {

	if res := lr.data.db.Table("location").
		Where("id>?", id).
		Where("col > 1").
		Where("update_status=?", 0).
		Updates(map[string]interface{}{"col": gorm.Expr("col - ?", 1), "update_status": 1}); res.Error != nil {
		return res.Error
	}

	if res := lr.data.db.Table("location").
		Where("id>?", id).
		Where("col = 1").
		Where("update_status=?", 0).
		Updates(map[string]interface{}{"row": gorm.Expr("row - ?", 1), "col": 3, "update_status": 1}); res.Error != nil {
		return res.Error
	}

	if res := lr.data.db.Table("location").
		Where("id>?", id).
		Updates(map[string]interface{}{"update_status": 0}); res.Error != nil {
		return res.Error
	}

	if res := lr.data.db.Table("location").
		Where("id=?", id).
		Updates(map[string]interface{}{"stop_is_update": 1}); res.Error != nil {
		return res.Error
	}
	return nil
}

// GetRewardLocationByRowOrCol .
func (lr *LocationRepo) GetRewardLocationByRowOrCol(ctx context.Context, row int64, col int64, locationRowConfig int64) ([]*biz.Location, error) {
	var (
		rowMin    int64 = 1
		rowMax    int64
		locations []*Location
	)
	if row > locationRowConfig {
		rowMin = row - locationRowConfig
	}
	rowMax = row + locationRowConfig

	if err := lr.data.db.Table("location").
		Where("status=?", "running").
		Where("row=? or (col=? and row>=? and row<=?)", row, col, rowMin, rowMax).
		Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	res := make([]*biz.Location, 0)
	for _, location := range locations {
		res = append(res, &biz.Location{
			ID:           location.ID,
			UserId:       location.UserId,
			Status:       location.Status,
			CurrentLevel: location.CurrentLevel,
			Current:      location.Current,
			CurrentMax:   location.CurrentMax,
			Row:          location.Row,
			Col:          location.Col,
			StopDate:     location.StopDate,
		})
	}

	return res, nil
}

// GetRewardLocationByIds .
func (lr *LocationRepo) GetRewardLocationByIds(ctx context.Context, ids ...int64) (map[int64]*biz.Location, error) {
	var locations []*Location
	if err := lr.data.db.Table("location").
		Where("status=?", "running").
		Where("id IN (?)", ids).
		Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	res := make(map[int64]*biz.Location, 0)
	for _, location := range locations {
		res[location.ID] = &biz.Location{
			ID:           location.ID,
			UserId:       location.UserId,
			Status:       location.Status,
			CurrentLevel: location.CurrentLevel,
			Current:      location.Current,
			CurrentMax:   location.CurrentMax,
			Row:          location.Row,
			Col:          location.Col,
		}
	}

	return res, nil
}

// GetLocations .
func (lr *LocationRepo) GetLocations(ctx context.Context, b *biz.Pagination, userId int64) ([]*biz.LocationNew, error, int64) {
	var (
		locations []*Location
		count     int64
	)
	instance := lr.data.db.Table("location_new").Where("status=?", "running")

	if 0 < userId {
		instance = instance.Where("user_id=?", userId)
	}

	instance = instance.Count(&count)
	if err := instance.Scopes(Paginate(b.PageNum, b.PageSize)).Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found"), 0
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error()), 0
	}

	res := make([]*biz.LocationNew, 0)
	for _, location := range locations {
		res = append(res, &biz.LocationNew{
			ID:         location.ID,
			UserId:     location.UserId,
			Status:     location.Status,
			Current:    location.Current,
			CurrentMax: location.CurrentMax,
			CreatedAt:  location.CreatedAt,
		})
	}

	return res, nil, count
}

// GetUserBalanceRecords .
func (lr *LocationRepo) GetUserBalanceRecords(ctx context.Context, b *biz.Pagination, userId int64, coinType string) ([]*biz.UserBalanceRecord, error, int64) {
	var (
		records []*UserBalanceRecord
		count   int64
	)

	instance := lr.data.db.Table("user_balance_record")
	if "" != coinType {
		instance = instance.Where("type = ? and coin_type=?", "deposit", coinType)
	} else {
		instance = instance.Where("type = ? and (coin_type=? or coin_type=? or coin_type = ?)", "deposit", "USDT", "HBS", "CSD")
	}

	if 0 < userId {
		instance = instance.Where("user_id=?", userId)
	}

	instance = instance.Count(&count)
	if err := instance.Scopes(Paginate(b.PageNum, b.PageSize)).Order("id desc").Find(&records).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found"), 0
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error()), 0
	}

	res := make([]*biz.UserBalanceRecord, 0)
	for _, v := range records {
		res = append(res, &biz.UserBalanceRecord{
			ID:        v.ID,
			UserId:    v.UserId,
			Amount:    v.Amount,
			CoinType:  v.CoinType,
			CreatedAt: v.CreatedAt,
		})
	}

	return res, nil, count
}

// GetLocationsAll .
func (lr *LocationRepo) GetLocationsAll(ctx context.Context, b *biz.Pagination, userId int64) ([]*biz.LocationNew, error, int64) {
	var (
		locations []*LocationNew
		count     int64
	)
	instance := lr.data.db.Table("location_new")

	if 0 < userId {
		instance = instance.Where("user_id=?", userId)
	}

	instance = instance.Count(&count)
	if err := instance.Scopes(Paginate(b.PageNum, b.PageSize)).Order("id desc").Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found"), 0
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error()), 0
	}

	res := make([]*biz.LocationNew, 0)
	for _, location := range locations {
		res = append(res, &biz.LocationNew{
			ID:         location.ID,
			UserId:     location.UserId,
			Status:     location.Status,
			Current:    location.Current,
			CurrentMax: location.CurrentMax,
			CreatedAt:  location.CreatedAt,
		})
	}

	return res, nil, count
}

// GetLocationUserCount .
func (lr *LocationRepo) GetLocationUserCount(ctx context.Context) int64 {
	var (
		count int64
	)
	lr.data.db.Table("location_new").Group("user_id").Count(&count)
	return count
}

// GetLocationByIds .
func (lr *LocationRepo) GetLocationByIds(ctx context.Context, userIds ...int64) ([]*biz.LocationNew, error) {
	var locations []*LocationNew
	if err := lr.data.db.Table("location_new").
		Where("user_id IN (?)", userIds).
		Find(&locations).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound("LOCATION_NOT_FOUND", "location not found")
		}

		return nil, errors.New(500, "LOCATION ERROR", err.Error())
	}

	res := make([]*biz.LocationNew, 0)
	for _, location := range locations {
		res = append(res, &biz.LocationNew{
			ID:         location.ID,
			UserId:     location.UserId,
			Status:     location.Status,
			Current:    location.Current,
			CurrentMax: location.CurrentMax,
		})
	}

	return res, nil
}
