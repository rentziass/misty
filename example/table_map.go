package main

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/icrowley/fake"

	"github.com/sirupsen/logrus"

	"github.com/rentziass/misty"
)

const allowedChars = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"
const datetimeFormat = "2006-01-02 15:04:05.000000"
const defaultPassword = "$2a$10$IbrSRwzHGTxLdT/Uml0v4eMkUCzFILx1e0splIV6aoO/dtjzSQB.G"

func main() {
	f, err := os.Open("dump.sql")
	if err != nil {
		panic(err)
	}

	targets := crmTargets()
	targets = append(targets, platformTargets()...)
	targets = append(targets, trackingTargets()...)

	o := misty.NewObfuscator(f, f, os.Stdout, targets, misty.WithMaxRoutines(5), misty.WithLogger(logrus.New()))
	err = o.Run()
	fmt.Println(err)

	//maps := misty.BuildTablesMap(f)
	//for _, m := range maps {
	//	if m.Name == "public.tag_types" {
	//		fmt.Println(m.Name, m.StartingLine, m.StartingAt, m.EndingAt)
	//		b := make([]byte, m.EndingAt-m.StartingAt)
	//		_, err := f.ReadAt(b, m.StartingAt)
	//		if err != nil {
	//			panic(err)
	//		}
	//		fmt.Println(string(b))
	//	}
	//}
}

func platformTargets() []*misty.Target {
	return []*misty.Target{
		{
			TableName: "public.members",
			Columns: []*misty.TargetColumn{
				{
					Name:  "first_name",
					Value: randomFirstName,
				},
				{
					Name:  "last_name",
					Value: randomLastName,
				},
				{
					Name:  "email",
					Value: randomEmailAddress,
				},
				{
					Name:  "mobile",
					Value: randomPhoneNumber,
				},
				{
					Name:  "twitter_id",
					Value: randomUsername,
				},
				{
					Name:  "unconfirmed_email",
					Value: nilValue,
				},
				{
					Name:  "company",
					Value: randomCompanyName,
				},
				{
					Name: "encrypted_password",
					Value: func(_ []byte) []byte {
						// Usual fixed password: c0d1ngsk1lls
						return []byte(defaultPassword)
					},
				},
			},
		},
		{
			TableName: "public.users",
			Columns: []*misty.TargetColumn{
				{
					Name: "encrypted_password",
					Value: func(_ []byte) []byte {
						// Usual fixed password: c0d1ngsk1lls
						return []byte(defaultPassword)
					},
				},
			},
		},
		{
			TableName: "public.authentications",
			Columns: []*misty.TargetColumn{
				{
					Name:  "provider_data",
					Value: nilValue,
				},
			},
		},
		{
			TableName: "public.attendees",
			Columns: []*misty.TargetColumn{
				{
					Name:  "first_name",
					Value: randomFirstName,
				},
				{
					Name:  "last_name",
					Value: randomLastName,
				},
				{
					Name:  "email",
					Value: randomEmailAddress,
				},
				{
					Name:  "company",
					Value: randomCompanyName,
				},
				{
					Name:  "phone",
					Value: randomPhoneNumber,
				},
			},
		},
		{
			TableName: "public.orders",
			Columns: []*misty.TargetColumn{
				{
					Name:  "title",
					Value: staticString("Ms"),
				},
				{
					Name:  "first_name",
					Value: randomFirstName,
				},
				{
					Name:  "last_name",
					Value: randomLastName,
				},
				{
					Name:  "email",
					Value: randomEmailAddress,
				},
				{
					Name:  "phone",
					Value: randomPhoneNumber,
				},
				{
					Name:  "po_number",
					Value: staticString("PO Number"),
				},
				{
					Name:  "po_value",
					Value: staticString("PO Value"),
				},
				{
					Name:  "sage_pay_vpst_id",
					Value: emptyString,
				},
				{
					Name:  "sage_pay_response",
					Value: staticString("null"),
				},
				{
					Name:  "sage_pay_tx_auth_no",
					Value: emptyString,
				},
				{
					Name:  "invoice_note",
					Value: emptyString,
				},
			},
		},
		{
			TableName: "public.invoices",
			Columns: []*misty.TargetColumn{
				{
					Name:  "po_number",
					Value: staticString("PO Number"),
				},
				{
					Name:  "po_value",
					Value: staticString("PO Value"),
				},
				{
					Name:  "first_name",
					Value: randomFirstName,
				},
				{
					Name:  "last_name",
					Value: randomLastName,
				},
				{
					Name:  "booking_name",
					Value: randomFullName,
				},
				{
					Name:  "company_name",
					Value: randomCompanyName,
				},
				{
					Name:  "vat_number",
					Value: staticString("UK12345678"),
				},
				{
					Name:  "discount_caption",
					Value: staticString("Discount Caption"),
				},
				{
					Name:  "promo_code",
					Value: randomPromoCode,
				},
				{
					Name:  "invoice_note",
					Value: emptyString,
				},
			},
		},
		{
			TableName: "public.addresses",
			Columns: []*misty.TargetColumn{
				{
					Name:  "company_name",
					Value: randomCompanyName,
				},
				{
					Name:  "address1",
					Value: randomStreetAddress,
				},
				{
					Name:  "address2",
					Value: emptyString,
				},
				{
					Name:  "address3",
					Value: emptyString,
				},
				{
					Name:  "city",
					Value: randomCity,
				},
				{
					Name:  "postcode",
					Value: randomZipCode,
				},
				{
					Name:  "email",
					Value: randomEmailAddress,
				},
				{
					Name:  "fax",
					Value: nilValue,
				},
			},
		},
		{
			TableName: "public.promo_codes",
			Columns: []*misty.TargetColumn{
				{
					Name:  "code",
					Value: randomPromoCode,
				},
				{
					Name:  "invoice_description",
					Value: staticString("Invoice Description"),
				},
			},
		},
		{
			TableName: "public.skills_vouchers",
			Columns: []*misty.TargetColumn{
				{
					Name:  "code",
					Value: randomSkillsVoucherCode("FAKE"),
				},
				{
					Name:  "access_token",
					Value: randomHex(16),
				},
				{
					Name:  "owner_first_name",
					Value: randomFirstName,
				},
				{
					Name:  "owner_last_name",
					Value: randomLastName,
				},
				{
					Name:  "owner_email",
					Value: randomEmailAddress,
				},
			},
		},
		{
			TableName: "public.versions",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName: "id",
					ShouldDelete: func(_ []byte) bool {
						return true
					},
				},
			},
		},
	}
}

func litePlatformTargets() []*misty.Target {
	return []*misty.Target{
		{
			TableName: "public.authentications",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName: "id",
					ShouldDelete: func(_ []byte) bool {
						return true
					},
				},
			},
		},
		{
			TableName: "public.tracking_events",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName: "id",
					ShouldDelete: func(_ []byte) bool {
						return true
					},
				},
			},
		},
		{
			TableName: "public.activity_tracking_events",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName: "id",
					ShouldDelete: func(_ []byte) bool {
						return true
					},
				},
			},
		},
		{
			TableName: "public.activity_profile_tag_scores",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName: "id",
					ShouldDelete: func(_ []byte) bool {
						return true
					},
				},
			},
		},
		{
			TableName: "public.activity_profile_content_views",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName: "id",
					ShouldDelete: func(_ []byte) bool {
						return true
					},
				},
			},
		},
		{
			TableName: "public.activity_tracking_visits",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName: "id",
					ShouldDelete: func(_ []byte) bool {
						return true
					},
				},
			},
		},
		{
			TableName: "public.activity_profile_exclusions",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName: "id",
					ShouldDelete: func(_ []byte) bool {
						return true
					},
				},
			},
		},
		{
			TableName: "public.activity_profiles",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName: "id",
					ShouldDelete: func(_ []byte) bool {
						return true
					},
				},
			},
		},
		{
			TableName: "public.trackings",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName: "id",
					ShouldDelete: func(_ []byte) bool {
						return true
					},
				},
			},
		},
		{
			TableName: "ffcrm.comments",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName: "id",
					ShouldDelete: func(_ []byte) bool {
						return true
					},
				},
			},
		},
	}
}

func trackingTargets() []*misty.Target {
	// 1 week
	trackingCutOff := time.Now().Add(-7 * 24 * time.Hour)

	// 30 days
	activityCutOff := time.Now().Add(-30 * 24 * time.Hour)
	return []*misty.Target{
		{
			TableName: "public.trackings",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName:   "created_at",
					ShouldDelete: olderThan(trackingCutOff),
				},
			},
		},
		{
			TableName: "public.tracking_events",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName:   "created_at",
					ShouldDelete: olderThan(trackingCutOff),
				},
			},
		},
		{
			TableName: "public.activity_tracking_events",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName:   "occurred_at",
					ShouldDelete: olderThan(activityCutOff),
				},
			},
		},
		{
			TableName: "public.activity_tracking_visits",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName:   "created_at",
					ShouldDelete: olderThan(activityCutOff),
				},
			},
		},
		{
			TableName: "public.activity_profile_content_views",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName:   "occurred_at",
					ShouldDelete: olderThan(activityCutOff),
				},
			},
		},
	}
}

func crmTargets() []*misty.Target {
	return []*misty.Target{
		{
			TableName: "ffcrm.accounts",
			Columns: []*misty.TargetColumn{
				{
					Name:  "name",
					Value: randomCompanyName,
				},
				{
					Name:  "website",
					Value: randomDomainName,
				},
				{
					Name:  "phone",
					Value: randomPhoneNumber,
				},
				{
					Name:  "fax",
					Value: nilValue,
				},
				{
					Name:  "email",
					Value: randomEmailAddress,
				},
			},
		},
		{
			TableName: "ffcrm.addresses",
			Columns: []*misty.TargetColumn{
				{
					Name:  "street1",
					Value: randomStreetAddress,
				},
				{
					Name:  "street2",
					Value: emptyString,
				},
				{
					Name:  "city",
					Value: randomCity,
				},
				{
					Name:  "state",
					Value: emptyString,
				},
				{
					Name:  "zipcode",
					Value: randomZipCode,
				},
				{
					Name:  "full_address",
					Value: nilValue,
				},
			},
		},
		{
			TableName: "ffcrm.contacts",
			Columns: []*misty.TargetColumn{
				{
					Name:  "first_name",
					Value: randomFirstName,
				},
				{
					Name:  "last_name",
					Value: randomLastName,
				},
				{
					Name:  "email",
					Value: randomEmailAddress,
				},
				{
					Name:  "alt_email",
					Value: emptyString,
				},
				{
					Name:  "phone",
					Value: randomPhoneNumber,
				},
				{
					Name:  "mobile",
					Value: randomPhoneNumber,
				},
				{
					Name:  "fax",
					Value: nilValue,
				},
				{
					Name:  "blog",
					Value: nilValue,
				},
				{
					Name:  "linkedin",
					Value: nilValue,
				},
				{
					Name:  "facebook",
					Value: nilValue,
				},
				{
					Name:  "skype",
					Value: nilValue,
				},
				{
					Name:  "twitter",
					Value: randomUsername,
				},
				{
					Name:  "web_company",
					Value: randomCompanyName,
				},
			},
		},
		{
			TableName: "ffcrm.leads",
			Columns: []*misty.TargetColumn{
				{
					Name:  "first_name",
					Value: randomFirstName,
				},
				{
					Name:  "last_name",
					Value: randomLastName,
				},
				{
					Name:  "company",
					Value: randomCompanyName,
				},
				{
					Name:  "email",
					Value: randomEmailAddress,
				},
				{
					Name:  "referred_by",
					Value: emptyString,
				},
				{
					Name:  "alt_email",
					Value: emptyString,
				},
				{
					Name:  "phone",
					Value: randomPhoneNumber,
				},
				{
					Name:  "mobile",
					Value: randomPhoneNumber,
				},
				{
					Name:  "blog",
					Value: nilValue,
				},
				{
					Name:  "linkedin",
					Value: nilValue,
				},
				{
					Name:  "facebook",
					Value: nilValue,
				},
				{
					Name:  "skype",
					Value: nilValue,
				},
				{
					Name:  "twitter",
					Value: randomUsername,
				},
			},
		},
		{
			TableName: "ffcrm.versions",
			DeleteRowRules: []*misty.DeleteRule{
				{
					ColumnName: "id",
					ShouldDelete: func(_ []byte) bool {
						return true
					},
				},
			},
		},
	}
}

var usernameCounter, emailCounter int

func randomUsername(_ []byte) []byte {
	u := fmt.Sprintf("%s%v", fake.UserName(), usernameCounter)
	usernameCounter++
	return []byte(u)
}

func randomFirstName(_ []byte) []byte {
	return []byte(fake.FirstName())
}

func randomLastName(_ []byte) []byte {
	return []byte(fake.LastName())
}

func randomFullName(_ []byte) []byte {
	return []byte(fake.FullName())
}

func randomEmailAddress(_ []byte) []byte {
	u := fmt.Sprintf("%s%v", fake.UserName(), emailCounter)
	emailCounter++
	return []byte(strings.ToLower(u + "@" + fake.DomainName()))
}

func randomPhoneNumber(_ []byte) []byte {
	return []byte(fake.Phone())
}

func randomCompanyName(_ []byte) []byte {
	return []byte(fake.Company())
}

func randomDomainName(_ []byte) []byte {
	return []byte(fake.DomainName())
}

func randomStreetAddress(_ []byte) []byte {
	return []byte(fake.StreetAddress())
}

func randomCity(_ []byte) []byte {
	return []byte(fake.City())
}

func randomZipCode(_ []byte) []byte {
	return []byte(fake.Zip())
}

func randomPromoCode(_ []byte) []byte {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	n := fmt.Sprintf("%v", r.Intn(99999)+1)
	for len(n) < 5 {
		n = n + "0"
	}

	return []byte("SKM_PROMO_" + n)
}

func randomSkillsVoucherCode(prefix string) func([]byte) []byte {
	return func(oldValue []byte) []byte {
		oldValueStr := string(oldValue)
		if oldValueStr == "" {
			return []byte("")
		}

		if oldValueStr == "\\N" {
			return nilValue(oldValue)
		}

		str := prefix
		// generate three strings of len 4 using allowed characters
		for i := 0; i < 3; i++ {
			b := make([]byte, 4)
			for i := range b {
				b[i] = allowedChars[rand.Int63()%int64(len(allowedChars))]
			}
			str = str + "-" + string(b)
		}

		return []byte(str)
	}
}

func randomHex(n int) func(_ []byte) []byte {
	return func(_ []byte) []byte {
		bytes := make([]byte, n)
		rand.Read(bytes)
		return []byte(hex.EncodeToString(bytes))
	}
}

func nilValue(_ []byte) []byte {
	return []byte("\\N")
}

func emptyString(_ []byte) []byte {
	return []byte("")
}

func olderThan(t time.Time) func([]byte) bool {
	return func(b []byte) bool {
		dbTime, err := time.Parse(datetimeFormat, string(b))
		if err != nil {
			return false
		}

		return dbTime.Before(t)
	}
}

func staticString(s string) func(_ []byte) []byte {
	return func(_ []byte) []byte {
		return []byte(s)
	}
}
