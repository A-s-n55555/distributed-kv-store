package server

import (
	"context"

	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// requirePromoted checks whether normal membership installation completed.
func (s *StagingService) requirePromoted() error {
	s.target.writeMu.Lock()
	defer s.target.writeMu.Unlock()

	expected := s.joinCandidate.Identity()
	if expected.Epoch == 0 ||
		s.target.activeMembership.Identity() != expected {
		return status.Error(
			codes.Unimplemented,
			"node is still staging",
		)
	}
	return nil
}

func (s *StagingService) Put(
	ctx context.Context,
	request *kvpb.PutRequest,
) (*kvpb.PutResponse, error) {
	if err := s.requirePromoted(); err != nil {
		return nil, err
	}
	return s.target.Put(ctx, request)
}

func (s *StagingService) Get(
	ctx context.Context,
	request *kvpb.GetRequest,
) (*kvpb.GetResponse, error) {
	if err := s.requirePromoted(); err != nil {
		return nil, err
	}
	return s.target.Get(ctx, request)
}

func (s *StagingService) Delete(
	ctx context.Context,
	request *kvpb.DeleteRequest,
) (*kvpb.DeleteResponse, error) {
	if err := s.requirePromoted(); err != nil {
		return nil, err
	}
	return s.target.Delete(ctx, request)
}

func (s *StagingService) Resolve(
	ctx context.Context,
	request *kvpb.ResolveRequest,
) (*kvpb.ResolveResponse, error) {
	if err := s.requirePromoted(); err != nil {
		return nil, err
	}
	return s.target.Resolve(ctx, request)
}

func (s *StagingService) GetMembershipStatus(
	ctx context.Context,
	request *kvpb.MembershipStatusRequest,
) (*kvpb.MembershipStatusResponse, error) {
	if err := s.requirePromoted(); err != nil {
		return nil, err
	}
	return s.target.GetMembershipStatus(ctx, request)
}
func (s *StagingService) AntiEntropySummary(
	ctx context.Context,
	request *kvpb.AntiEntropySummaryRequest,
) (*kvpb.AntiEntropySummaryResponse, error) {
	if err := s.requirePromoted(); err != nil {
		return nil, err
	}
	return s.target.AntiEntropySummary(ctx, request)
}

func (s *StagingService) AntiEntropyBucket(
	ctx context.Context,
	request *kvpb.AntiEntropyBucketRequest,
) (*kvpb.AntiEntropyBucketResponse, error) {
	if err := s.requirePromoted(); err != nil {
		return nil, err
	}
	return s.target.AntiEntropyBucket(ctx, request)
}
