package sstore

import (
	"context"
	"net"

	"github.com/waltage/sstore/pb"
	"google.golang.org/grpc"
)

type SStoreServer struct {
	Address string
	Opts    []grpc.ServerOption
	GServer *grpc.Server
	service *RPC
}

func NewSStoreServer(dpath, dname string,
	opts ...grpc.ServerOption) *SStoreServer {
	ss := SStoreServer{
		GServer: grpc.NewServer(opts...),
		Opts:    opts,
	}
	db := newSStore(dpath, dname)
	ss.service = &RPC{
		db: db,
	}
	pb.RegisterSStoreRPCServer(ss.GServer, ss.service)
	return &ss
}

func (ss *SStoreServer) Serve(address string) {
	if address != "" {
		ss.Address = address
	}
	lis, err := net.Listen("tcp", ss.Address)
	if err != nil {
		svLog.Error(err)
	}
	defer ss.service.db.close()

	ss.GServer.Serve(lis)
}

type RPC struct {
	pb.UnimplementedSStoreRPCServer
	db *sstore
}

func (r *RPC) GetNewKey(ctx context.Context, in *pb.Bucket) (*pb.Key, error) {
	k := r.db.NewKey(in.Name)
	ret := keyToPBKey(k)
	return ret, nil
}

func (r *RPC) ListBuckets(in *pb.Empty,
	stream pb.SStoreRPC_ListBucketsServer) error {
	bucks, err := r.db.ListBuckets()
	if err != nil {
		svLog.Warn(err)
		return err
	}
	for _, b := range bucks {
		err := stream.Send(&pb.Bucket{Name: b})
		if err != nil {
			svLog.Warn(err)
		}
	}
	return nil
}

func (r *RPC) ListKeys(in *pb.Bucket,
	stream pb.SStoreRPC_ListKeysServer) error {
	keys, err := r.db.ListKeys(in.Name)
	if err != nil {
		svLog.Warn(err)
		return err
	}
	for _, k := range keys {
		err := stream.Send(keyToPBKey(k))
		if err != nil {
			svLog.Warn(err)
		}
	}
	return nil
}

func (r *RPC) Search(in *pb.Key, stream pb.SStoreRPC_SearchServer) error {
	if in.Bucket.Name == "" {
		in.Bucket.Name = "DEFAULT"
	}
	keys, err := r.db.SearchKeys(in.Bucket.Name, in.Id)
	if err != nil {
		svLog.Warn(err)
		return err
	}
	for _, k := range keys {
		err := stream.Send(keyToPBKey(k))
		if err != nil {
			svLog.Warn(err)
		}
	}
	return nil
}

func (r *RPC) Version(ctx context.Context, in *pb.Key) (*pb.Key, error) {
	vz, err := r.db.Version(pbKeytoKey(in))
	if err != nil {
		svLog.Warn(err)
		return in, err
	}
	in.Version = vz
	return in, nil
}

func (r *RPC) Put(ctx context.Context, in *pb.Object) (*pb.Key, error) {
	nKey := pbKeytoKey(in.Key)
	nBytes := pbStorable{in.Payload}

	retKey, err := r.db.Put(nKey, &nBytes)
	if err != nil {
		svLog.Warn(err)
		return nil, err
	}

	return keyToPBKey(retKey), nil
}

func (r *RPC) Get(ctx context.Context, in *pb.Key) (*pb.Object, error) {
	nKey := pbKeytoKey(in)
	var temp pbStorable

	err := r.db.Get(nKey, &temp)
	if err != nil {
		svLog.Warn(err)
		return nil, err
	}
	obj := &pb.Object{
		Key:     in,
		Payload: temp.Encode(),
	}
	return obj, nil
}

func (r *RPC) Delete(ctx context.Context, in *pb.Key) (*pb.Empty, error) {
	nKey := pbKeytoKey(in)
	err := r.db.Delete(nKey)
	if err != nil {
		svLog.Warn(err)
		return nil, err
	}
	return &pb.Empty{}, nil
}
