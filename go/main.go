package main

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/uber/h3-go/v4"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	Resolution     = 8
	SearchLat      = 23.8103
	SearchLng      = 90.4125
	UserBaseLatMin = 21.5
	UserBaseLatMax = 26.3
	UserBaseLngMin = 88.5
	UserBaseLngMax = 92.2
	TotalUsers     = 4000000
	BatchSize      = 10000
)

type User struct {
	ID      uint    `gorm:"primaryKey"`
	Name    string  `gorm:"size:50"`
	Lat     float64 `gorm:"type:float"`
	Lng     float64 `gorm:"type:float"`
	H3Index string  `gorm:"size:15;index"`
}

func main() {
	db, err := gorm.Open(sqlite.Open("../h3_users.db"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		panic("failed to connect database")
	}

	db.AutoMigrate(&User{})

	var count int64
	db.Model(&User{}).Count(&count)

	if count < TotalUsers {
		fmt.Printf("Populating database with %d users...\n", TotalUsers-int(count))
		populateDatabase(db, int(TotalUsers-int(count)))
	}

	radiusKm := 10.0
	start := time.Now()

	results, err := findNearbyUsers(db, SearchLat, SearchLng, radiusKm, 300)
	if err != nil {
		fmt.Printf("Error searching: %v\n", err)
		return
	}

	elapsed := time.Since(start)
	fmt.Printf("Found %d users within %.1fkm of Dhaka center\n", len(results), radiusKm)
	fmt.Printf("Took %s\n", elapsed)

	if len(results) > 0 {
		fmt.Printf("Example result: %s at (%.4f, %.4f)\n", results[0].Name, results[0].Lat, results[0].Lng)
	}
}

func populateDatabase(db *gorm.DB, totalToCreate int) {
	for i := 0; i < totalToCreate; i += BatchSize {
		size := BatchSize
		if i+BatchSize > totalToCreate {
			size = totalToCreate - i
		}

		batch := make([]User, size)
		for j := 0; j < size; j++ {
			lat := UserBaseLatMin + rand.Float64()*(UserBaseLatMax-UserBaseLatMin)
			lng := UserBaseLngMin + rand.Float64()*(UserBaseLngMax-UserBaseLngMin)

			latLng := h3.NewLatLng(lat, lng)

			cell, err := h3.LatLngToCell(latLng, Resolution)
			if err != nil {

				continue
			}

			batch[j] = User{
				Name:    fmt.Sprintf("User_%d", i+j),
				Lat:     lat,
				Lng:     lng,
				H3Index: cell.String(),
			}
		}

		db.CreateInBatches(batch, BatchSize)
		if (i/BatchSize)%10 == 0 {
			fmt.Printf("Inserted %d/%d...\n", i+size, totalToCreate)
		}
	}
}

func findNearbyUsers(db *gorm.DB, sLat, sLng float64, radiusKm float64, maxUsersRequired int) ([]User, error) {
	centerLatLng := h3.NewLatLng(sLat, sLng)
	centerCell, err := h3.LatLngToCell(centerLatLng, Resolution)
	if err != nil {
		return nil, fmt.Errorf("invalid search coordinates: %v", err)
	}

	edgeLen, err := h3.HexagonEdgeLengthAvgKm(Resolution)
	if err != nil {
		return nil, err
	}

	// Maximum possible K we are willing to search
	maxK := int(math.Ceil(radiusKm / (edgeLen * 1.5)))
	fmt.Printf("will look at %d rings for %.1fkm\n", maxK, radiusKm)
	var finalResults []User

	// Iterate through rings one by one (k=0 is the center cell)
	for k := 0; k <= maxK; k++ {
		var ringHexes []h3.Cell

		if k == 0 {
			ringHexes = []h3.Cell{centerCell}
		} else {
			// GridRing returns ONLY the hexagons at exactly distance k
			ringHexes, err = h3.GridRing(centerCell, k)
			if err != nil {
				// If we hit a pentagon, GridRing returns an error.
				// The likelihood of this is very small given there are only 11 pentagons.
				// We fallback to GridDiskDistances which is slower but "Pentagon-safe".
				// We only take the outermost ring [k]
				allRings, diskErr := h3.GridDiskDistances(centerCell, k)
				if diskErr != nil {
					return nil, diskErr
				}
				ringHexes = allRings[k]
			}
		}

		fmt.Printf("found %d hex cells at %d K ring\n", len(ringHexes), k)

		// Convert only this ring to strings
		hexStrings := make([]string, len(ringHexes))
		for j, hex := range ringHexes {
			hexStrings[j] = hex.String()
		}

		// Query only the "new" area
		var potentialUsers []User
		err = db.Where("h3_index IN ?", hexStrings).Find(&potentialUsers).Error
		if err != nil {
			return nil, err
		}

		// Filter and check count
		for _, u := range potentialUsers {
			uLatLng := h3.NewLatLng(u.Lat, u.Lng)
			dist := h3.GreatCircleDistanceKm(centerLatLng, uLatLng)

			if dist <= radiusKm {
				finalResults = append(finalResults, u)
				if len(finalResults) >= maxUsersRequired {
					return finalResults, nil
				}
			}
		}

		// Optional: Check if the current ring's distance already exceeds radiusKm
		// to stop even before reaching maxK
		currentRingDist := float64(k) * edgeLen * 1.5
		if currentRingDist > radiusKm && len(finalResults) > 0 {
			break
		}
	}

	return finalResults, fmt.Errorf("only found %d/%d users", len(finalResults), maxUsersRequired)
}
