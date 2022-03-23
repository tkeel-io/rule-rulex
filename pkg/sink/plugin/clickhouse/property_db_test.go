package clickhouse

//var (
//	test_sql = `INSERT INTO %s.%s (
//time,
//user_id,
//device_id,
//source_id,
//thing_id,
//identifier,
//value_int32,
//value_float,
//value_double,
//value_string,
//value_enum,
//value_bool,
//value_string_ex,
//value_array_string,
//value_array_int32,
//value_array_float,
//value_array_double,
//action_date,
//action_time)
//VALUES (?,?,?, ?, ?, ?,  ?, ?,?, ?,?,?, ?, ?, ?, ?, ?,?,?)`
//)
//
//func TestNewPropertyDB(t *testing.T) {
//	type args struct {
//		dbName string
//		table  string
//		urls   []string
//	}
//	tests := []struct {
//		name         string
//		args         args
//		wantProperty sink.DBImpl
//		wantErr      bool
//	}{
//		{
//			name: "createEvent",
//			args: args{
//				urls:   []string{},
//				dbName: "iot_manage",
//				table:  "device_thing_data",
//			},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			gotProperty, err := NewPropertyDB(tt.args.dbName, tt.args.table, tt.args.urls)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("NewPropertyDB() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(gotProperty, tt.wantProperty) {
//				t.Errorf("NewPropertyDB() gotProperty = %v, want %v", gotProperty, tt.wantProperty)
//			}
//		})
//	}
//}
//
//func TestPropertyDB_BatchCreate(t *testing.T) {
//	type fields struct {
//		balance LoadBalance
//		dbName  string
//		table   string
//	}
//	type args struct {
//		ctx       context.Context
//		sqlCreate string
//		values    interface{}
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		wantErr bool
//	}{
//		{
//			name: "createProperty",
//			fields: fields{
//				balance: NewLoadBalanceRandom([]*Server{}),
//				dbName:  "iot_manage",
//				table:   "device_thing_data",
//			},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			p := &PropertyDB{
//				balance: tt.fields.balance,
//				dbName:  tt.fields.dbName,
//				table:   tt.fields.table,
//			}
//			if err := p.BatchCreate(tt.args.ctx, tt.args.sqlCreate, tt.args.values); (err != nil) != tt.wantErr {
//				t.Errorf("BatchCreate() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}
//
//func TestPropertyDB_Close(t *testing.T) {
//	type fields struct {
//		balance LoadBalance
//		dbName  string
//		table   string
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		wantErr bool
//	}{
//		{
//			name: "createProperty",
//			fields: fields{
//				balance: NewLoadBalanceRandom([]*Server{}),
//				dbName:  "iot_manage",
//				table:   "device_thing_data",
//			},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			p := &PropertyDB{
//				balance: tt.fields.balance,
//				dbName:  tt.fields.dbName,
//				table:   tt.fields.table,
//			}
//			if err := p.Close(); (err != nil) != tt.wantErr {
//				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}
//func buildTestProperty(num int) []*entity.DeviceModel {
//	result := make([]*entity.DeviceModel, num)
//	t := 1579401376072
//	for i := 0; i < num; i++ {
//		result[i] = &entity.DeviceModel{
//			UserId:           "hexing",
//			ThingId:          "xxxxx",
//			DeviceId:         "asasas",
//			SourceId:         "asasasa",
//			Identifier:       "sssssss",
//			ValueInt32:       int32(rand.Int()),
//			ValueFloat:       0,
//			ValueDouble:      0,
//			ValueEnum:        0,
//			ValueString:      "",
//			ValueBool:        false,
//			ValueStringEx:    "",
//			ValueArrayString: []string{},
//			ValueArrayInt32:  []int32{},
//			ValueArrayFloat:  []float32{},
//			ValueArrayDouble: []float64{},
//			Time:             int64(t),
//			ActionDate:       time.Time{},
//			ActionTime:       time.Time{},
//		}
//		t++
//	}
//	return result
//}
//func BenchmarkPropertyDB_BatchCreate(b *testing.B) {
//	gotProperty, err := NewPropertyDB("iot_manage", "device_thing_data", []string{"http://default@127.0.0.1:9000?max_execution_time=3&enable_http_compression=1"})
//	if err != nil {
//		fmt.Println(err)
//	}
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		data := buildTestProperty(200000)
//		err = gotProperty.BatchCreate(context.Background(), test_sql, data)
//		if err != nil {
//			fmt.Println(err)
//		}
//	}
//}
//func TestPropertyDB_BatchCreate2(t *testing.T) {
//	gotProperty, err := NewPropertyDB("iot_manage", "device_thing_data", []string{"http://default@127.0.0.1:9000?max_execution_time=3"})
//	if err != nil {
//		fmt.Println(err)
//	}
//	pool, err := ants.NewPool(5)
//	assert.Nil(t, err)
//	var count uint64
//	start := time.Now()
//	for {
//		pool.Submit(func() {
//			defer func() {
//				count++
//				if count%10 == 0 {
//					fmt.Println(count)
//					fmt.Println(time.Since(start))
//				}
//			}()
//			data := buildTestProperty(100000)
//			err = gotProperty.BatchCreate(context.Background(), test_sql, data)
//			if err != nil {
//				fmt.Println(err)
//			}
//		})
//	}
//	//var wg sync.WaitGroup
//	//wg.Add(5)
//	//for i := 0; i < 5; i++ {
//	//	go func() {
//	//		defer wg.Done()
//	//		data := buildTestProperty(100000)
//	//		err = gotProperty.BatchCreate(context.Background(), test_sql, data)
//	//		if err != nil {
//	//			fmt.Println(err)
//	//		}
//	//	}()
//	//}
//	//wg.Wait()
//}
//func TestPropertyDB_BatchCreate3(t *testing.T) {
//	gotProperty, err := NewPropertyDB("iot_manage", "device_thing_data", []string{"tcp://default@127.0.0.1:9000?max_execution_time=3"})
//	if err != nil {
//		fmt.Println(err)
//	}
//	var wg sync.WaitGroup
//	wg.Add(5)
//	for i := 0; i < 5; i++ {
//		go func() {
//			defer wg.Done()
//			data := buildTestProperty(200000)
//			err = gotProperty.BatchCreate(context.Background(), test_sql, data)
//			if err != nil {
//				fmt.Println(err)
//			}
//		}()
//	}
//	wg.Wait()
//}
//func TestPropertyDB_BatchCreate4(t *testing.T) {
//	gotProperty, err := NewPropertyDB("iot_manage", "device_thing_data", []string{"http://default@127.0.0.1:9000?max_execution_time=3&enable_http_compression=1&debug=1"})
//	if err != nil {
//		fmt.Println(err)
//	}
//	var wg sync.WaitGroup
//	wg.Add(1)
//	for i := 0; i < 1; i++ {
//		go func() {
//			defer wg.Done()
//			data := buildTestProperty(20000)
//			err = gotProperty.BatchCreate(context.Background(), test_sql, data)
//			if err != nil {
//				fmt.Println(err)
//			}
//		}()
//	}
//	wg.Wait()
//}
//func BenchmarkFunctionSome(b *testing.B) {
//	gotProperty, err := NewPropertyDB("iot_manage", "device_thing_data", []string{"http://default@127.0.0.1:8123?max_execution_time=3&enable_http_compression=1"})
//	if err != nil {
//		fmt.Println(err)
//	}
//	for i := 0; i < 10; i++ {
//		b.RunParallel(func(pb *testing.PB) {
//			for pb.Next() {
//				data := buildTestProperty(20000)
//				err = gotProperty.BatchCreate(context.Background(), test_sql, data)
//				if err != nil {
//					fmt.Println(err)
//				}
//			}
//		})
//	}
//
//}
