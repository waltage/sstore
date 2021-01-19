package sstore

import (
	"context"
	"io"
	"os"

	logz "github.com/waltage/dwio-logz"
	"github.com/waltage/sstore/pb"
	"google.golang.org/grpc"
)

var clog = logz.NewLog(os.Stdout, "sstore.client", logz.Info)

type SStoreClient struct {
	Address string
	Options []grpc.DialOption
	Conn    *grpc.ClientConn
	Client  pb.SStoreRPCClient
}

func (ss *SStoreClient) Connect(addr string, opts ...grpc.DialOption) {
	ss.Address = addr
	if len(opts) == 0 {
		ss.Options = []grpc.DialOption{grpc.WithInsecure()}
	} else {
		ss.Options = opts
	}
	conn, err := grpc.Dial(addr, ss.Options...)
	if err != nil {
		clog.Error(err)
	}

	ss.Conn = conn
	ss.Client = pb.NewSStoreRPCClient(ss.Conn)
}

func (ss *SStoreClient) Close() {
	ss.Conn.Close()
}

func (ss *SStoreClient) NewKey(bucket string) (*Key, error) {
	ctx := context.Background()
	req := pb.Bucket{Name: bucket}
	resp, err := ss.Client.GetNewKey(ctx, &req)
	if err != nil {
		clLog.Warn(err)
		return nil, err
	}
	return pbKeytoKey(resp), nil
}

func (ss *SStoreClient) ListBuckets() ([]string, error) {
	ctx := context.Background()
	req := pb.Empty{}
	ret := make([]string, 0)

	respStream, err := ss.Client.ListBuckets(ctx, &req)
	if err != nil {
		clLog.Warn(err)
		return ret, err
	}

	for {
		bucket, err := respStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			clLog.Warn(err)
			return ret, err
		}
		ret = append(ret, bucket.Name)
	}
	return ret, nil
}

func (ss *SStoreClient) ListKeys(bucket string) ([]*Key, error) {
	ctx := context.Background()
	req := pb.Bucket{Name: bucket}
	ret := make([]*Key, 0)

	respStream, err := ss.Client.ListKeys(ctx, &req)
	if err != nil {
		clLog.Warn(err)
		return ret, err
	}
	for {
		key, err := respStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			clLog.Warn(err)
			return ret, err
		}
		ret = append(ret, pbKeytoKey(key))
	}
	return ret, nil
}

func (ss *SStoreClient) Search(bucket, prefix string) ([]*Key, error) {
	ctx := context.Background()
	req := pb.Key{
		Bucket: &pb.Bucket{Name: bucket},
		Id:     prefix,
	}
	ret := make([]*Key, 0)

	respStream, err := ss.Client.Search(ctx, &req)
	if err != nil {
		clLog.Warn(err)
		return ret, err
	}
	for {
		key, err := respStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			clLog.Warn(err)
			return ret, err
		}
		ret = append(ret, pbKeytoKey(key))
	}
	return ret, nil
}

func (ss *SStoreClient) Version(key *Key) (uint64, error) {
	ctx := context.Background()
	req := keyToPBKey(key)

	resp, err := ss.Client.Version(ctx, req)
	if err != nil {
		clLog.Warn(err)
		return 0, err
	}
	return resp.Version, nil

}

func (ss *SStoreClient) Put(key *Key, obj Storable) (*Key, error) {
	ctx := context.Background()
	req := &pb.Object{
		Key:     keyToPBKey(key),
		Payload: obj.Encode(),
	}
	resp, err := ss.Client.Put(ctx, req)
	if err != nil {
		clLog.Warn(err)
		return nil, err
	}
	return pbKeytoKey(resp), nil
}

func (ss *SStoreClient) Get(key *Key, obj Storable) error {
	ctx := context.Background()
	req := keyToPBKey(key)
	resp, err := ss.Client.Get(ctx, req)
	if err != nil {
		clLog.Warn(err)
		return err
	}
	if len(resp.Payload) == 0 {
		e := ef("key not found")
		clLog.Info(e)
		return e
	}
	err = obj.Decode(resp.Payload)
	if err != nil {
		clLog.Warn(err)
		return err
	}
	return nil
}

func (ss *SStoreClient) Delete(key *Key) error {
	ctx := context.Background()
	req := keyToPBKey(key)
	_, err := ss.Client.Delete(ctx, req)
	if err != nil {
		clLog.Warn(err)
		return err
	}
	return nil
}
